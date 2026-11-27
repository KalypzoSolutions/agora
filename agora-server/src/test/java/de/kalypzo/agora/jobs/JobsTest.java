package de.kalypzo.agora.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.proto.v1.DocChanged;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-13: expire and idempotency cleanup, with an injected clock. */
class JobsTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    static final Instant NOW = Instant.parse("2026-11-27T12:00:00Z");
    final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @BeforeAll
    static void start() {
        postgres.start();
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(postgres.getJdbcUrl());
        cfg.setUsername(postgres.getUsername());
        cfg.setPassword(postgres.getPassword());
        cfg.setMaximumPoolSize(4);
        ds = new HikariDataSource(cfg);
        Migrations.migrate(ds);
    }

    @AfterAll
    static void stop() {
        ds.close();
        postgres.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("TRUNCATE pantheon_doc, pantheon_idempotency");
        }
    }

    private void insertDoc(String store, String id, Instant expiresAt) throws SQLException {
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO pantheon_doc (store, id, expires_at) VALUES (?, ?, ?)")) {
            ps.setString(1, store);
            ps.setString(2, id);
            ps.setTimestamp(3, expiresAt == null ? null : Timestamp.from(expiresAt));
            ps.executeUpdate();
        }
    }

    private void insertKey(String key, Instant createdAt) throws SQLException {
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO pantheon_idempotency (idempotency_key, result, created_at) VALUES (?, '{}', ?)")) {
            ps.setString(1, key);
            ps.setTimestamp(2, Timestamp.from(createdAt));
            ps.executeUpdate();
        }
    }

    private List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    @Test
    void expireJobDeletesAndPublishes() throws Exception {
        insertDoc("lobby:clans", "gone1", NOW.minusSeconds(1));
        insertDoc("lobby:clans", "gone-at-now", NOW);
        insertDoc("lobby:parties", "gone2", NOW.minus(Duration.ofHours(1)));
        insertDoc("lobby:clans", "later", NOW.plusSeconds(1));
        insertDoc("lobby:clans", "never", null);
        List<DocChanged> published = new ArrayList<>();

        new ExpireJob(ds, published::add, clock).run();

        assertThat(column("SELECT store || '/' || id FROM pantheon_doc ORDER BY 1"))
                .containsExactly("lobby:clans/later", "lobby:clans/never");
        assertThat(published).hasSize(3).allSatisfy(c -> {
            assertThat(c.getDeleted()).isTrue();
            assertThat(c.getVersion()).isPositive();
            assertThat(c.getIdempotencyKey()).isEmpty();
        });
        assertThat(published.stream().map(c -> c.getRef().getStoreDoc().getStore() + "/" + c.getRef().getStoreDoc().getId()))
                .containsExactlyInAnyOrder("lobby:clans/gone1", "lobby:clans/gone-at-now", "lobby:parties/gone2");
        assertThat(published.stream().map(DocChanged::getVersion).distinct()).hasSize(3);

        published.clear();
        new ExpireJob(ds, published::add, clock).run();
        assertThat(published).isEmpty();
    }

    @Test
    void expireJobDeletesMoreRowsThanOneBatch() throws Exception {
        for (int i = 0; i < 7; i++) insertDoc("lobby:clans", "d" + i, NOW.minusSeconds(1));
        insertDoc("lobby:clans", "keep", NOW.plusSeconds(1));
        List<DocChanged> published = new ArrayList<>();

        new ExpireJob(ds, published::add, clock, 3).run();

        assertThat(column("SELECT id FROM pantheon_doc")).containsExactly("keep");
        assertThat(published).hasSize(7).allMatch(DocChanged::getDeleted);
    }

    @Test
    void publishFailureDoesNotDropTheRest() throws Exception {
        for (int i = 0; i < 3; i++) insertDoc("lobby:clans", "d" + i, NOW.minusSeconds(1));
        List<DocChanged> published = new ArrayList<>();

        new ExpireJob(ds, c -> {
            published.add(c);
            if (published.size() == 1) throw new IllegalStateException("boom");
        }, clock).run();

        assertThat(published).hasSize(3);
    }

    @Test
    void cleanupKeepsKeysYoungerThan24h() throws Exception {
        assertThat(IdempotencyCleanupJob.RETENTION).isEqualTo(Duration.ofHours(24));
        insertKey("young", NOW.minus(Duration.ofHours(23)).minus(Duration.ofMinutes(59)));
        insertKey("old", NOW.minus(Duration.ofHours(24)).minus(Duration.ofMinutes(1)));

        new IdempotencyCleanupJob(ds, clock).run();

        assertThat(column("SELECT idempotency_key FROM pantheon_idempotency")).containsExactly("young");
    }

    @Test
    void cleanupDeletesMoreRowsThanOneBatch() throws Exception {
        // A-13: bounded batches (each its own short statement) until a batch comes back short.
        for (int i = 0; i < 7; i++) insertKey("old" + i, NOW.minus(Duration.ofHours(25)));
        insertKey("young", NOW.minus(Duration.ofHours(1)));
        java.util.concurrent.atomic.AtomicInteger connections = new java.util.concurrent.atomic.AtomicInteger();
        javax.sql.DataSource counting = (javax.sql.DataSource) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {javax.sql.DataSource.class}, (proxy, m, args) -> {
                    if (m.getName().equals("getConnection")) connections.incrementAndGet();
                    try {
                        return m.invoke(ds, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        new IdempotencyCleanupJob(counting, clock, 3).run();

        assertThat(column("SELECT idempotency_key FROM pantheon_idempotency")).containsExactly("young");
        assertThat(connections).hasValue(3); // 3 + 3 + 1
    }
}

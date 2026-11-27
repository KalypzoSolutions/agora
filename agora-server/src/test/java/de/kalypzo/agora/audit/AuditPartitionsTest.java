package de.kalypzo.agora.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.db.Migrations;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-16: daily partitions of pantheon_audit. */
class AuditPartitionsTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    static Clock at(String instant) {
        return Clock.fixed(Instant.parse(instant), ZoneOffset.UTC);
    }

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
    void clean() throws Exception {
        for (String p : partitions()) {
            try (Connection c = ds.getConnection(); var st = c.createStatement()) {
                st.execute("DROP TABLE " + p);
            }
        }
    }

    static List<String> partitions() throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT inhrelid::regclass::text FROM pg_inherits"
                        + " WHERE inhparent = 'pantheon_audit'::regclass ORDER BY 1")) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    @Test
    void ensureCreatesTodayAndTwoAhead() throws Exception {
        new AuditPartitions(ds, at("2026-11-27T12:00:00Z")).ensure(2);
        assertThat(partitions())
                .containsExactly("pantheon_audit_20261127", "pantheon_audit_20261128", "pantheon_audit_20261129");
    }

    @Test
    void ensureIsIdempotent() throws Exception {
        AuditPartitions p = new AuditPartitions(ds, at("2026-11-27T12:00:00Z"));
        p.ensure(2);
        p.ensure(2);
        assertThat(partitions()).hasSize(3);
    }

    @Test
    void dropsOnlyFullyExpiredPartitions() throws Exception {
        new AuditPartitions(ds, at("2026-11-27T12:00:00Z")).ensure(2);
        new AuditPartitions(ds, at("2026-12-12T00:30:00Z")).dropExpired(Duration.ofDays(14));
        assertThat(partitions()).containsExactly("pantheon_audit_20261128", "pantheon_audit_20261129");
    }

    @Test
    void postgresOrdersByTime() throws Exception {
        new AuditPartitions(ds, at("2026-11-27T12:00:00Z")).ensure(2);
        UUID a = UuidV7.lowerBound(Instant.parse("2026-11-27T01:00:00Z"));
        UUID b = UuidV7.lowerBound(Instant.parse("2026-11-27T23:59:59.999Z"));
        UUID c = UuidV7.lowerBound(Instant.parse("2026-11-28T00:00:00Z"));
        for (UUID id : List.of(c, a, b)) {
            try (Connection con = ds.getConnection();
                    PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO pantheon_audit (id, player, server, action) VALUES (?, ?, 's', 'a:b')")) {
                ps.setObject(1, id);
                ps.setObject(2, new UUID(0, 1));
                ps.executeUpdate();
            }
        }
        List<UUID> read = new ArrayList<>();
        try (Connection con = ds.getConnection();
                var st = con.createStatement();
                ResultSet rs = st.executeQuery("SELECT id FROM pantheon_audit ORDER BY id")) {
            while (rs.next()) read.add(rs.getObject(1, UUID.class));
        }
        assertThat(read).containsExactly(a, b, c);
    }

    @Test
    void cutoffBoundaryIsExact() throws Exception {
        new AuditPartitions(ds, at("2026-11-27T12:00:00Z")).ensure(1);
        // 20261127 ends 2026-11-28T00:00Z: one millisecond before the cutoff it stays, at the cutoff it goes
        new AuditPartitions(ds, at("2026-12-11T23:59:59.999Z")).dropExpired(Duration.ofDays(14));
        assertThat(partitions()).containsExactly("pantheon_audit_20261127", "pantheon_audit_20261128");
        new AuditPartitions(ds, at("2026-12-12T00:00:00Z")).dropExpired(Duration.ofDays(14));
        assertThat(partitions()).containsExactly("pantheon_audit_20261128");
    }

    @Test
    void concurrentEnsureAndDropDoNotFail() throws Exception {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            for (int round = 0; round < 5; round++) {
                clean();
                AuditPartitions p = new AuditPartitions(ds, at("2026-11-27T12:00:00Z"));
                AuditPartitions later = new AuditPartitions(ds, at("2026-12-12T00:30:00Z"));
                var go = new java.util.concurrent.CountDownLatch(1);
                List<java.util.concurrent.Future<?>> fs = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    fs.add(pool.submit(() -> { go.await(); p.ensure(2); return null; }));
                }
                fs.add(pool.submit(() -> { go.await(); later.dropExpired(Duration.ofDays(14)); return null; }));
                go.countDown();
                for (var f : fs) f.get(30, java.util.concurrent.TimeUnit.SECONDS);
                p.ensure(2);
                assertThat(partitions()).contains("pantheon_audit_20261128", "pantheon_audit_20261129");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waitForAdvisoryLockEndsAtLockTimeout() throws Exception {
        AuditPartitions p = new AuditPartitions(ds, at("2026-11-27T12:00:00Z"), Duration.ofMillis(300));
        try (Connection holder = ds.getConnection(); var st = holder.createStatement()) {
            holder.setAutoCommit(false);
            st.execute("SELECT pg_advisory_xact_lock(" + 0x70616e5f61756431L + ")");
            long t0 = System.nanoTime();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.ensure(2))
                    .isInstanceOf(IllegalStateException.class);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.dropExpired(Duration.ofDays(14)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(5));
            holder.rollback();
        }
        p.ensure(2); // lock free again
        assertThat(partitions()).hasSize(3);
    }
}

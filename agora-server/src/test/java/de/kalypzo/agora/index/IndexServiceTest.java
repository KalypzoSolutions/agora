package de.kalypzo.agora.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.proto.v1.*;
import de.kalypzo.agora.sql.SortSql;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class IndexServiceTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;
    IndexService service;

    static Value lv(long v) {
        return Value.newBuilder().setLongValue(v).build();
    }

    static FieldSpec field(String ns, String name, ScalarType t, Value def) {
        FieldSpec.Builder b = FieldSpec.newBuilder().setNamespace(ns).setName(name).setType(t);
        if (def != null) b.setDefaultValue(def);
        return b.build();
    }

    static IndexDeclaration players(FieldSpec f, boolean unique) {
        return IndexDeclaration.newBuilder()
                .setTarget(QueryTarget.newBuilder().setPlayers(true)).setField(f).setUnique(unique).build();
    }

    static IndexDeclaration store(String store, FieldSpec f, boolean unique) {
        return IndexDeclaration.newBuilder()
                .setTarget(QueryTarget.newBuilder().setStore(store)).setField(f).setUnique(unique).build();
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
    void setUp() throws Exception {
        for (String idx : strings("SELECT indexname FROM pg_indexes WHERE indexname NOT LIKE '%\\_pkey' "
                + "AND (indexname LIKE 'pantheon\\_player\\_%' OR indexname LIKE 'pantheon\\_doc\\_%\\_%')")) {
            exec("DROP INDEX IF EXISTS \"" + idx + "\"");
        }
        exec("TRUNCATE pantheon_index, pantheon_player, pantheon_doc");
        service = new IndexService(ds);
    }

    static void exec(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static List<String> strings(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection c = ds.getConnection(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    static String one(String sql) throws SQLException {
        return strings(sql).get(0);
    }

    static String name(CompletableFuture<String> f) throws Exception {
        return f.get(30, TimeUnit.SECONDS);
    }

    @Test
    void declaresOnceForRepeatedDeclarations() throws Exception {
        IndexDeclaration d = players(field("economy", "coins", ScalarType.LONG, lv(0)), false);
        assertThat(name(service.declare(d))).isEqualTo("pantheon_player_economy_coins");
        String created = one("SELECT created_at::text FROM pantheon_index WHERE name = 'pantheon_player_economy_coins'");
        assertThat(one("SELECT status FROM pantheon_index WHERE name = 'pantheon_player_economy_coins'")).isEqualTo("ready");

        assertThat(name(service.declare(d))).isEqualTo("pantheon_player_economy_coins");
        assertThat(name(service.declare(d))).isEqualTo("pantheon_player_economy_coins");

        assertThat(one("SELECT created_at::text FROM pantheon_index WHERE name = 'pantheon_player_economy_coins'"))
                .isEqualTo(created);
        assertThat(strings("SELECT indexname FROM pg_indexes WHERE indexname = 'pantheon_player_economy_coins'"))
                .hasSize(1);
        assertThat(strings("SELECT 1 FROM pantheon_index")).hasSize(1);
        assertThat(one("SELECT indexdef FROM pg_indexes WHERE indexname = 'pantheon_player_economy_coins'"))
                .contains("namespace = 'economy'");
    }

    @Test
    void storeIndexIsNamedAndScoped() throws Exception {
        IndexDeclaration d = store("lobby:clans", field("lobby:clans", "owner", ScalarType.STRING, null), true);
        assertThat(name(service.declare(d))).isEqualTo("pantheon_doc_lobby_clans_owner");
        assertThat(one("SELECT indexdef FROM pg_indexes WHERE indexname = 'pantheon_doc_lobby_clans_owner'"))
                .contains("UNIQUE").contains("store = 'lobby:clans'");
    }

    @Test
    void uniqueWithDuplicatesFails() throws Exception {
        exec("INSERT INTO pantheon_doc(store, id, data) VALUES ('lobby:clans','a','{\"owner\":\"x\"}'),"
                + "('lobby:clans','b','{\"owner\":\"x\"}')");
        IndexDeclaration d = store("lobby:clans", field("lobby:clans", "owner", ScalarType.STRING, null), true);

        assertThatThrownBy(() -> name(service.declare(d)))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IndexDeclarationException.class)
                .satisfies(e -> assertThat(((IndexDeclarationException) e.getCause()).duplicate()).isTrue());

        assertThat(one("SELECT status FROM pantheon_index WHERE name = 'pantheon_doc_lobby_clans_owner'"))
                .isEqualTo("failed");
        assertThat(one("SELECT error FROM pantheon_index WHERE name = 'pantheon_doc_lobby_clans_owner'")).isNotBlank();
        assertThat(strings("SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                + "WHERE NOT i.indisvalid")).isEmpty();
    }

    @Test
    void failedIndexIsRetriedAfterFix() throws Exception {
        exec("INSERT INTO pantheon_doc(store, id, data) VALUES ('lobby:clans','a','{\"owner\":\"x\"}'),"
                + "('lobby:clans','b','{\"owner\":\"x\"}')");
        IndexDeclaration d = store("lobby:clans", field("lobby:clans", "owner", ScalarType.STRING, null), true);
        assertThatThrownBy(() -> name(service.declare(d))).isInstanceOf(ExecutionException.class);
        String created = one("SELECT created_at::text FROM pantheon_index WHERE name = 'pantheon_doc_lobby_clans_owner'");

        exec("DELETE FROM pantheon_doc WHERE id = 'b'");
        assertThat(name(service.declare(d))).isEqualTo("pantheon_doc_lobby_clans_owner");

        assertThat(one("SELECT status FROM pantheon_index WHERE name = 'pantheon_doc_lobby_clans_owner'"))
                .isEqualTo("ready");
        assertThat(one("SELECT coalesce(error, 'none') FROM pantheon_index WHERE name = 'pantheon_doc_lobby_clans_owner'"))
                .isEqualTo("none");
        assertThat(one("SELECT created_at::text FROM pantheon_index WHERE name = 'pantheon_doc_lobby_clans_owner'"))
                .isEqualTo(created);
        assertThat(strings("SELECT indexname FROM pg_indexes WHERE indexname = 'pantheon_doc_lobby_clans_owner'"))
                .hasSize(1);
    }

    @Test
    void concurrentDeclarationsShareOneFuture() throws Exception {
        IndexDeclaration d = players(field("economy", "gems", ScalarType.LONG, lv(0)), false);
        CompletableFuture<String> a = service.declare(d);
        CompletableFuture<String> b = service.declare(d);
        assertThat(name(a)).isEqualTo(name(b));
        assertThat(strings("SELECT 1 FROM pantheon_index")).hasSize(1);
    }

    @Test
    void sortQueryUsesIndex() throws Exception {
        FieldSpec score = field("lobby:clans", "score", ScalarType.LONG, lv(0));
        exec("INSERT INTO pantheon_doc(store, id, data) SELECT 'lobby:clans', 'c' || g, "
                + "jsonb_build_object('score', (g * 7919) % 10007) FROM generate_series(1, 10000) g");
        exec("INSERT INTO pantheon_doc(store, id, data) SELECT 'other:store', 'o' || g, '{}' "
                + "FROM generate_series(1, 100) g");
        name(service.declare(store("lobby:clans", score, false)));
        exec("ANALYZE pantheon_doc");

        // same shape as ReadService.query for a store target
        String order = SortSql.orderBy(f -> "d", List.of(Sort.newBuilder().setField(score).build()), "d.id", true)
                .sql();
        String sql = "SELECT d.id, d.data::text, d.version FROM pantheon_doc d WHERE d.store = 'lobby:clans' "
                + "AND (d.expires_at IS NULL OR d.expires_at > now()) " + order + " LIMIT 10";
        StringBuilder plan = new StringBuilder();
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute("SET enable_seqscan = off");
            try (ResultSet rs = s.executeQuery("EXPLAIN " + sql)) {
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
            }
        }
        assertThat(plan.toString()).contains("pantheon_doc_lobby_clans_score");
    }

    @Test
    void longNamesAreShortenedDeterministically() throws Exception {
        String ns = "n".repeat(30);
        String fld = "f".repeat(30);
        IndexDeclaration d = players(field(ns, fld, ScalarType.LONG, lv(0)), false);
        String full = "pantheon_player_" + ns + "_" + fld;
        assertThat(full.length()).isGreaterThan(63);

        String n1 = IndexNames.of(d);
        assertThat(n1).hasSize(63).startsWith(full.substring(0, 54) + "_").matches(".*_[0-9a-f]{8}");
        assertThat(IndexNames.of(d)).isEqualTo(n1);
        assertThat(IndexNames.of(players(field(ns, fld + "x", ScalarType.LONG, lv(0)), false))).isNotEqualTo(n1);

        assertThat(name(service.declare(d))).isEqualTo(n1);
        assertThat(strings("SELECT indexname FROM pg_indexes WHERE indexname = '" + n1 + "'")).hasSize(1);
    }

    @Test
    void invalidNamesAreRejected() {
        IndexDeclaration d = players(field("Eco;--", "coins", ScalarType.LONG, null), false);
        assertThatThrownBy(() -> service.declare(d)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rpcMapsDuplicatesToAlreadyExists() throws Exception {
        exec("INSERT INTO pantheon_doc(store, id, data) VALUES ('lobby:clans','a','{\"owner\":\"x\"}'),"
                + "('lobby:clans','b','{\"owner\":\"x\"}')");
        de.kalypzo.agora.service.AgoraService svc = new de.kalypzo.agora.service.AgoraService();
        de.kalypzo.agora.grpc.DataEndpoints.registerIndexes(svc, service);
        String n = "idx-" + System.nanoTime();
        io.grpc.Server server = io.grpc.inprocess.InProcessServerBuilder.forName(n).addService(svc).build().start();
        io.grpc.ManagedChannel ch = io.grpc.inprocess.InProcessChannelBuilder.forName(n).build();
        try {
            AgoraGrpc.AgoraBlockingStub stub = AgoraGrpc.newBlockingStub(ch).withDeadlineAfter(10, TimeUnit.SECONDS);
            IndexDeclaration d = store("lobby:clans", field("lobby:clans", "owner", ScalarType.STRING, null), true);
            assertThatThrownBy(() -> stub.declareIndex(DeclareIndexRequest.newBuilder().setIndex(d).build()))
                    .isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.ALREADY_EXISTS));
            IndexDeclaration ok = players(field("economy", "coins", ScalarType.LONG, lv(0)), false);
            assertThat(stub.declareIndex(DeclareIndexRequest.newBuilder().setIndex(ok).build()).getName())
                    .isEqualTo("pantheon_player_economy_coins");
        } finally {
            ch.shutdownNow();
            server.shutdownNow();
        }
    }
}

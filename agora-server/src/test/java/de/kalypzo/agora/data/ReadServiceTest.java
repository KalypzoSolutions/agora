package de.kalypzo.agora.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.grpc.DataEndpoints;
import de.kalypzo.agora.proto.v1.*;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class ReadServiceTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    static final FieldSpec LEVEL = FieldSpec.newBuilder().setNamespace("lobby").setName("level")
            .setType(ScalarType.LONG).setDefaultValue(lv(1)).build();
    static final FieldSpec NAME = FieldSpec.newBuilder().setNamespace("pantheon").setName("name")
            .setType(ScalarType.STRING).build();
    static final FieldSpec COINS = FieldSpec.newBuilder().setNamespace("economy").setName("coins")
            .setType(ScalarType.LONG).setDefaultValue(lv(0)).build();
    static final FieldSpec SCORE = FieldSpec.newBuilder().setNamespace("lobby:clans").setName("score")
            .setType(ScalarType.LONG).setDefaultValue(lv(0)).build();


    final List<String> holderCalls = new ArrayList<>();
    Consumer<UUID> beforeRead = p -> {};
    Server server;
    ManagedChannel channel;
    AgoraGrpc.AgoraBlockingStub stub;

    static Value lv(long v) {
        return Value.newBuilder().setLongValue(v).build();
    }

    static Value sv(String v) {
        return Value.newBuilder().setStringValue(v).build();
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
        exec("TRUNCATE pantheon_player, pantheon_doc");
        holderCalls.clear();
        beforeRead = p -> {};
        AgoraService service = new AgoraService();
        HolderSink sink = (node, player) -> {
            holderCalls.add(node + ":" + player);
            beforeRead.accept(player);
        };
        DataEndpoints.register(service, new ReadService(ds, sink));
        ServerInterceptor identity = new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(
                    ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                Context ctx = Context.current().withValue(NodeIdentity.CONTEXT_KEY, new NodeIdentity("paper-1"));
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
        String name = "read-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name).addService(service).intercept(identity).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        stub = AgoraGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    static void exec(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static void player(UUID p, String ns, String json) throws SQLException {
        exec("INSERT INTO pantheon_player (player, namespace, data) VALUES ('" + p + "', '" + ns + "', '" + json
                + "'::jsonb)");
    }

    static void doc(String store, String id, String json, String expires) throws SQLException {
        exec("INSERT INTO pantheon_doc (store, id, data, expires_at) VALUES ('" + store + "', '" + id + "', '" + json
                + "'::jsonb, " + (expires == null ? "NULL" : "now() + interval '" + expires + "'") + ")");
    }

    static QueryTarget players() {
        return QueryTarget.newBuilder().setPlayers(true).build();
    }

    static QueryTarget store(String s) {
        return QueryTarget.newBuilder().setStore(s).build();
    }

    static Condition gte(FieldSpec f, long v) {
        return Condition.newBuilder().setField(f)
                .setPredicate(Predicate.newBuilder().setGte(lv(v))).build();
    }

    static Sort desc(FieldSpec f) {
        return Sort.newBuilder().setField(f).setDescending(true).build();
    }

    static Sort asc(FieldSpec f) {
        return Sort.newBuilder().setField(f).build();
    }

    QueryResponse query(QueryTarget t, QuerySpec spec) {
        return stub.query(QueryRequest.newBuilder().setTarget(t).setSpec(spec).build());
    }

    static String playerOf(DocSnapshot d) {
        return d.getDoc().getPlayer();
    }

    // ---------- LoadPlayer / Load ----------

    @Test
    void loadPlayerRegistersHolderBeforeReading() throws Exception {
        UUID p = UUID.randomUUID();
        player(p, "pantheon", "{\"name\":\"Alex\"}");
        beforeRead = id -> {
            try {
                player(id, "race", "{\"laps\":3}");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        LoadPlayerResponse r = stub.loadPlayer(LoadPlayerRequest.newBuilder().setPlayer(p.toString()).build());
        assertThat(holderCalls).containsExactly("paper-1:" + p);
        assertThat(r.getRowsList()).extracting(row -> row.getRef().getPlayerRow().getNamespace())
                .containsExactly("pantheon", "race");
        Row race = r.getRows(1);
        assertThat(race.getDataMap().get("laps").getLongValue()).isEqualTo(3);
        assertThat(race.getVersion()).isPositive();
    }

    @Test
    void loadUnknownPlayerReturnsNoRows() {
        LoadPlayerResponse r = stub.loadPlayer(
                LoadPlayerRequest.newBuilder().setPlayer(UUID.randomUUID().toString()).build());
        assertThat(r.getRowsList()).isEmpty();
        LoadResponse l = stub.load(LoadRequest.newBuilder()
                .setDoc(DocRef.newBuilder().setPlayer(UUID.randomUUID().toString())).build());
        assertThat(l.getRowsList()).isEmpty();
    }

    @Test
    void loadDoesNotRegisterHolder() throws Exception {
        UUID p = UUID.randomUUID();
        player(p, "pantheon", "{\"name\":\"Alex\"}");
        LoadResponse l = stub.load(LoadRequest.newBuilder()
                .setDoc(DocRef.newBuilder().setPlayer(p.toString())).build());
        assertThat(l.getRowsCount()).isEqualTo(1);
        assertThat(holderCalls).isEmpty();
    }

    @Test
    void expiredStoreDocIsHidden() throws Exception {
        doc("lobby:clans", "live", "{\"score\":5}", "1 hour");
        doc("lobby:clans", "forever", "{\"score\":6}", null);
        doc("lobby:clans", "dead", "{\"score\":7}", "-1 hour");

        DocRef dead = DocRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore("lobby:clans").setId("dead")).build();
        DocRef live = DocRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore("lobby:clans").setId("live")).build();
        assertThat(stub.load(LoadRequest.newBuilder().setDoc(dead).build()).getRowsList()).isEmpty();
        LoadResponse l = stub.load(LoadRequest.newBuilder().setDoc(live).build());
        assertThat(l.getRowsCount()).isEqualTo(1);
        assertThat(l.getRows(0).getRef().getStoreDoc().getId()).isEqualTo("live");

        QueryResponse q = query(store("lobby:clans"), QuerySpec.getDefaultInstance());
        assertThat(q.getDocsList()).extracting(d -> d.getDoc().getStoreDoc().getId())
                .containsExactly("forever", "live");
        assertThat(stub.count(CountRequest.newBuilder().setTarget(store("lobby:clans")).build()).getCount())
                .isEqualTo(2);
    }

    // ---------- Query / Count ----------

    @Test
    void playerQueryUsesDefaultForMissingNamespaceRow() throws Exception {
        UUID a = UUID.randomUUID(), b = UUID.randomUUID();
        player(a, "pantheon", "{\"name\":\"A\"}");
        player(b, "pantheon", "{\"name\":\"B\"}");
        player(b, "lobby", "{\"level\":0}");
        QueryResponse q = query(players(), QuerySpec.newBuilder().addWhere(gte(LEVEL, 1)).build());
        assertThat(q.getDocsList()).extracting(ReadServiceTest::playerOf).containsExactly(a.toString());
        assertThat(stub.count(CountRequest.newBuilder().setTarget(players()).addWhere(gte(LEVEL, 1)).build())
                .getCount()).isEqualTo(1);
    }

    @Test
    void playerQueryReturnsAllNamespacesIncludingName() throws Exception {
        UUID a = UUID.randomUUID();
        player(a, "pantheon", "{\"name\":\"A\"}");
        player(a, "lobby", "{\"level\":4}");
        player(a, "economy", "{\"coins\":9}");
        QueryResponse q = query(players(), QuerySpec.newBuilder().addWhere(gte(LEVEL, 1)).build());
        assertThat(q.getDocsCount()).isEqualTo(1);
        DocSnapshot d = q.getDocs(0);
        assertThat(d.getRowsList()).extracting(r -> r.getRef().getPlayerRow().getNamespace())
                .containsExactlyInAnyOrder("pantheon", "lobby", "economy");
        Row pan = d.getRowsList().stream()
                .filter(r -> r.getRef().getPlayerRow().getNamespace().equals("pantheon")).findFirst().orElseThrow();
        assertThat(pan.getDataMap().get("name").getStringValue()).isEqualTo("A");
    }

    @Test
    void sortIsDeterministicOnTies() throws Exception {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            UUID p = UUID.randomUUID();
            ids.add(p);
            player(p, "pantheon", "{\"name\":\"n" + i + "\"}");
            player(p, "economy", "{\"coins\":10}");
        }
        QuerySpec spec = QuerySpec.newBuilder().addSort(desc(COINS)).build();
        List<String> first = query(players(), spec).getDocsList().stream().map(ReadServiceTest::playerOf).toList();
        List<String> second = query(players(), spec).getDocsList().stream().map(ReadServiceTest::playerOf).toList();
        assertThat(first).isEqualTo(second).hasSize(6);
        assertThat(first).isEqualTo(ids.stream().map(UUID::toString).sorted().toList());

        for (int i = 0; i < 4; i++) doc("lobby:clans", "d" + i, "{\"score\":1}", null);
        QueryResponse sd = query(store("lobby:clans"), QuerySpec.newBuilder().addSort(desc(SCORE)).build());
        assertThat(sd.getDocsList()).extracting(d -> d.getDoc().getStoreDoc().getId())
                .containsExactly("d0", "d1", "d2", "d3");
    }

    @Test
    void storeTieBreakIsBinary() throws Exception {
        for (String id : List.of("B", "a", "_c", "A")) doc("lobby:clans", id, "{\"score\":1}", null);
        QueryResponse q = query(store("lobby:clans"), QuerySpec.newBuilder().addSort(desc(SCORE)).build());
        assertThat(q.getDocsList()).extracting(d -> d.getDoc().getStoreDoc().getId())
                .containsExactly("A", "B", "_c", "a");
    }

    @Test
    void limitAbove1000IsRejected() {
        assertThatThrownBy(() -> query(players(), QuerySpec.newBuilder().setLimit(1001).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> query(players(), QuerySpec.newBuilder().setLimit(-1).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> query(players(), QuerySpec.newBuilder().setOffset(-1).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(query(players(), QuerySpec.newBuilder().setLimit(1000).build()).getDocsCount()).isZero();
    }

    @Test
    void defaultLimitIs100() throws Exception {
        for (int i = 0; i < 105; i++) doc("lobby:clans", "d" + i, "{}", null);
        assertThat(query(store("lobby:clans"), QuerySpec.getDefaultInstance()).getDocsCount()).isEqualTo(100);
    }

    @Test
    void offsetPagesWithoutGapsOrDuplicates() throws Exception {
        for (int i = 0; i < 7; i++) {
            UUID p = UUID.randomUUID();
            player(p, "pantheon", "{\"name\":\"n" + i + "\"}");
            player(p, "economy", "{\"coins\":" + (i % 3) + "}");
        }
        List<String> all = query(players(), QuerySpec.newBuilder().addSort(desc(COINS)).build())
                .getDocsList().stream().map(ReadServiceTest::playerOf).toList();
        List<String> paged = new ArrayList<>();
        for (int off = 0; off < 7; off += 3) {
            paged.addAll(query(players(), QuerySpec.newBuilder().addSort(desc(COINS)).setLimit(3).setOffset(off).build())
                    .getDocsList().stream().map(ReadServiceTest::playerOf).toList());
        }
        assertThat(all).hasSize(7).doesNotHaveDuplicates();
        assertThat(paged).isEqualTo(all);
    }

    // ---------- Versions ----------

    @Test
    void versionsReportZeroForMissingRow() throws Exception {
        UUID p = UUID.randomUUID();
        player(p, "economy", "{\"coins\":1}");
        doc("lobby:clans", "a", "{}", null);
        RowRef present = RowRef.newBuilder().setPlayerRow(
                PlayerRow.newBuilder().setPlayer(p.toString()).setNamespace("economy")).build();
        RowRef missing = RowRef.newBuilder().setPlayerRow(
                PlayerRow.newBuilder().setPlayer(p.toString()).setNamespace("race")).build();
        RowRef docPresent = RowRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore("lobby:clans").setId("a")).build();
        RowRef docMissing = RowRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore("lobby:clans").setId("zz")).build();
        VersionsResponse r = stub.versions(VersionsRequest.newBuilder()
                .addRows(present).addRows(missing).addRows(docPresent).addRows(docMissing).build());
        assertThat(r.getVersionsList()).extracting(RowVersion::getRef)
                .containsExactly(present, missing, docPresent, docMissing);
        assertThat(r.getVersions(0).getVersion()).isPositive();
        assertThat(r.getVersions(1).getVersion()).isZero();
        assertThat(r.getVersions(2).getVersion()).isPositive();
        assertThat(r.getVersions(3).getVersion()).isZero();
    }

    @Test
    void versionsForPlayersReturnAllRows() throws Exception {
        UUID p = UUID.randomUUID(), other = UUID.randomUUID();
        player(p, "pantheon", "{\"name\":\"P\"}");
        player(p, "economy", "{\"coins\":1}");
        player(other, "pantheon", "{\"name\":\"O\"}");
        VersionsResponse r = stub.versions(VersionsRequest.newBuilder().addPlayers(p.toString()).build());
        assertThat(r.getVersionsList()).extracting(v -> v.getRef().getPlayerRow().getNamespace())
                .containsExactlyInAnyOrder("pantheon", "economy");
        assertThat(r.getVersionsList()).allSatisfy(v -> {
            assertThat(v.getRef().getPlayerRow().getPlayer()).isEqualTo(p.toString());
            assertThat(v.getVersion()).isPositive();
        });
    }
}

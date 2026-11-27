package de.kalypzo.agora.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.grpc.DataEndpoints;
import de.kalypzo.agora.proto.v1.*;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class WriteServiceTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    static final String CLANS = "lobby:clans";
    static final String UNIQUE_INDEX = "pantheon_doc_lobby_clans_owner";
    static final FieldSpec COINS = FieldSpec.newBuilder().setNamespace("economy").setName("coins")
            .setType(ScalarType.LONG).setDefaultValue(lv(0)).build();
    static final FieldSpec KITS = FieldSpec.newBuilder().setNamespace("lobby").setName("kits")
            .setType(ScalarType.SET).build();
    static final FieldSpec SCORE = FieldSpec.newBuilder().setNamespace("lobby").setName("score")
            .setType(ScalarType.LONG).setDefaultValue(lv(0)).build();
    static final FieldSpec BANK = FieldSpec.newBuilder().setNamespace("lobby").setName("bank")
            .setType(ScalarType.LONG).setDefaultValue(lv(100)).build();
    static final FieldSpec OWNER = FieldSpec.newBuilder().setNamespace("lobby").setName("owner")
            .setType(ScalarType.STRING).build();
    static final FieldSpec FOREIGN = FieldSpec.newBuilder().setNamespace("economy").setName("gems")
            .setType(ScalarType.LONG).setDefaultValue(lv(0)).build();

    final List<DocChanged> published = new CopyOnWriteArrayList<>();
    /** For each published change: the version a fresh connection sees for its row at publish time. */
    final List<Long> visibleAtPublish = new CopyOnWriteArrayList<>();
    Server server;
    ManagedChannel channel;
    AgoraGrpc.AgoraBlockingStub raw;

    static Value lv(long v) {
        return Value.newBuilder().setLongValue(v).build();
    }

    static Value sv(String v) {
        return Value.newBuilder().setStringValue(v).build();
    }

    @BeforeAll
    static void start() throws SQLException {
        postgres.start();
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(postgres.getJdbcUrl());
        cfg.setUsername(postgres.getUsername());
        cfg.setPassword(postgres.getPassword());
        cfg.setMaximumPoolSize(16);
        ds = new HikariDataSource(cfg);
        Migrations.migrate(ds);
        exec("CREATE UNIQUE INDEX " + UNIQUE_INDEX + " ON pantheon_doc ((data->>'owner')) WHERE store = 'lobby:clans'");
    }

    @AfterAll
    static void stop() {
        ds.close();
        postgres.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        exec("TRUNCATE pantheon_player, pantheon_doc, pantheon_idempotency");
        published.clear();
        visibleAtPublish.clear();
        AgoraService service = new AgoraService();
        ChangePublisher publisher = change -> {
            visibleAtPublish.add(committedVersion(change.getRef()));
            published.add(change);
        };
        DataEndpoints.registerWrites(service, new WriteService(ds, publisher));
        String name = "write-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name).addService(service).executor(Executors.newCachedThreadPool())
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        raw = AgoraGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    AgoraGrpc.AgoraBlockingStub stub() {
        return raw.withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    // ---------- helpers ----------

    static void exec(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static long committedVersion(RowRef ref) {
        try (Connection c = ds.getConnection()) {
            PreparedStatement ps;
            if (ref.hasPlayerRow()) {
                ps = c.prepareStatement("SELECT version FROM pantheon_player WHERE player = ?::uuid AND namespace = ?");
                ps.setString(1, ref.getPlayerRow().getPlayer());
                ps.setString(2, ref.getPlayerRow().getNamespace());
            } else {
                ps = c.prepareStatement("SELECT version FROM pantheon_doc WHERE store = ? AND id = ?");
                ps.setString(1, ref.getStoreDoc().getStore());
                ps.setString(2, ref.getStoreDoc().getId());
            }
            try (ps; ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    static String key() {
        return UUID.randomUUID().toString();
    }

    static DocRef player(UUID p) {
        return DocRef.newBuilder().setPlayer(p.toString()).build();
    }

    static StoreDoc storeDoc(String id) {
        return StoreDoc.newBuilder().setStore(CLANS).setId(id).build();
    }

    static DocRef doc(String id) {
        return DocRef.newBuilder().setStoreDoc(storeDoc(id)).build();
    }

    static FieldOp inc(FieldSpec f, long d) {
        return FieldOp.newBuilder().setField(f).setInc(d).build();
    }

    static FieldOp withdraw(FieldSpec f, long a) {
        return FieldOp.newBuilder().setField(f).setWithdraw(a).build();
    }

    static FieldOp set(FieldSpec f, Value v) {
        return FieldOp.newBuilder().setField(f).setSet(v).build();
    }

    static FieldOp add(FieldSpec f, Value v) {
        return FieldOp.newBuilder().setField(f).setAdd(v).build();
    }

    static ApplyRequest apply(String key, DocRef doc, FieldOp... ops) {
        return ApplyRequest.newBuilder().setIdempotencyKey(key).setDoc(doc).addAllOps(List.of(ops))
                .setCreatedAtMillis(System.currentTimeMillis()).build();
    }

    ApplyResponse applyNow(DocRef doc, FieldOp... ops) {
        return stub().apply(apply(key(), doc, ops));
    }

    CreateResponse create(String key, String id, String field, Value v) {
        return stub().create(CreateRequest.newBuilder().setIdempotencyKey(key).setDoc(storeDoc(id))
                .putData(field, v).build());
    }

    boolean delete(String key, String id) {
        return stub().delete(DeleteRequest.newBuilder().setIdempotencyKey(key).setDoc(storeDoc(id)).build()).getDeleted();
    }

    void expire(String key, String id, long atMillis) {
        stub().expire(ExpireRequest.newBuilder().setIdempotencyKey(key).setDoc(storeDoc(id)).setAtMillis(atMillis).build());
    }

    TransferResponse transfer(String key, DocRef from, DocRef to, FieldSpec f, long amount) {
        return stub().transfer(TransferRequest.newBuilder().setIdempotencyKey(key).setFrom(from).setTo(to)
                .setField(f).setAmount(amount).build());
    }

    static long longOf(Row row, String field) {
        return row.getDataOrThrow(field).getLongValue();
    }

    long coins(UUID p) {
        RowRef ref = RowRef.newBuilder().setPlayerRow(PlayerRow.newBuilder().setPlayer(p.toString())
                .setNamespace("economy")).build();
        return storedLong(ref, "coins");
    }

    static long storedLong(RowRef ref, String field) {
        try (Connection c = ds.getConnection()) {
            PreparedStatement ps;
            if (ref.hasPlayerRow()) {
                ps = c.prepareStatement("SELECT (data->>?)::bigint FROM pantheon_player WHERE player = ?::uuid AND namespace = ?");
                ps.setString(1, field);
                ps.setString(2, ref.getPlayerRow().getPlayer());
                ps.setString(3, ref.getPlayerRow().getNamespace());
            } else {
                ps = c.prepareStatement("SELECT (data->>?)::bigint FROM pantheon_doc WHERE store = ? AND id = ?");
                ps.setString(1, field);
                ps.setString(2, ref.getStoreDoc().getStore());
                ps.setString(3, ref.getStoreDoc().getId());
            }
            try (ps; ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new AssertionError("no row " + ref);
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    static long count(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    static RowRef docRef(String id) {
        return RowRef.newBuilder().setStoreDoc(storeDoc(id)).build();
    }

    static Status.Code code(Runnable r) {
        try {
            r.run();
        } catch (StatusRuntimeException e) {
            return e.getStatus().getCode();
        }
        throw new AssertionError("call did not fail");
    }

    // ---------- card tests ----------

    @Test
    void concurrentWithdrawOnlyOneWins() throws Exception {
        UUID p = UUID.randomUUID();
        applyNow(player(p), set(COINS, lv(100)));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<ApplyResponse>> results = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                results.add(pool.submit(() -> {
                    go.await();
                    return applyNow(player(p), withdraw(COINS, 60));
                }));
            }
            go.countDown();
            int applied = 0;
            for (Future<ApplyResponse> f : results) {
                ApplyResponse r = f.get(10, TimeUnit.SECONDS);
                if (r.getApplied()) applied++;
                else assertThat(r.getRowsList()).isEmpty();
            }
            assertThat(applied).isEqualTo(1);
            assertThat(coins(p)).isEqualTo(40);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void replayReturnsStoredResultWithoutReapplying() {
        UUID p = UUID.randomUUID();
        ApplyRequest req = apply(key(), player(p), inc(COINS, 5));
        ApplyResponse first = stub().apply(req);
        ApplyResponse second = stub().apply(req);

        assertThat(second).isEqualTo(first);
        assertThat(longOf(first.getRows(0), "coins")).isEqualTo(5);
        assertThat(coins(p)).isEqualTo(5);
        assertThat(published).hasSize(1);
    }

    @Test
    void concurrentSameKeyAppliesOnce() throws Exception {
        UUID p = UUID.randomUUID();
        ApplyRequest req = apply(key(), player(p), inc(COINS, 5));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<ApplyResponse>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(pool.submit(() -> {
                    go.await();
                    return stub().apply(req);
                }));
            }
            go.countDown();
            ApplyResponse expected = results.getFirst().get(10, TimeUnit.SECONDS);
            for (Future<ApplyResponse> f : results) assertThat(f.get(10, TimeUnit.SECONDS)).isEqualTo(expected);
            assertThat(coins(p)).isEqualTo(5);
            assertThat(published).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void multiNamespaceAtomicRollsBack() throws Exception {
        UUID p = UUID.randomUUID();
        ApplyResponse r = applyNow(player(p), add(KITS, sv("gold")), withdraw(COINS, 10));

        assertThat(r.getApplied()).isFalse();
        assertThat(r.getRowsList()).isEmpty();
        assertThat(count("SELECT count(*) FROM pantheon_player")).isZero();
        assertThat(count("SELECT count(*) FROM pantheon_idempotency")).isZero();
    }

    @Test
    void changePublishedAfterCommitWithKey() {
        UUID p = UUID.randomUUID();
        ApplyResponse first = applyNow(player(p), inc(COINS, 1));
        published.clear();
        visibleAtPublish.clear();

        String k = key();
        ApplyResponse r = stub().apply(apply(k, player(p), inc(COINS, 2)));

        assertThat(published).hasSize(1);
        DocChanged c = published.getFirst();
        assertThat(c.getIdempotencyKey()).isEqualTo(k);
        assertThat(c.getVersion()).isGreaterThan(first.getRows(0).getVersion()).isEqualTo(r.getRows(0).getVersion());
        assertThat(c.getDeleted()).isFalse();
        assertThat(c.getRef()).isEqualTo(r.getRows(0).getRef());
        assertThat(c.getDataOrThrow("coins").getLongValue()).isEqualTo(3);
        assertThat(visibleAtPublish).containsExactly(c.getVersion()); // committed before publish
    }

    @Test
    void noChangePublishedOnRollback() {
        UUID p = UUID.randomUUID();
        assertThat(applyNow(player(p), withdraw(COINS, 1)).getApplied()).isFalse();
        assertThat(code(() -> applyNow(player(p), inc(COINS, Long.MAX_VALUE), inc(COINS, 0),
                set(KITS, Value.newBuilder().setLongValue(1).build())))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(published).isEmpty();
    }

    @Test
    void createSecondTimeReturnsFalse() {
        CreateResponse first = create(key(), "spawn", "score", lv(1));
        CreateResponse second = create(key(), "spawn", "score", lv(2));

        assertThat(first.getCreated()).isTrue();
        assertThat(longOf(first.getRow(), "score")).isEqualTo(1);
        assertThat(first.getRow().getRef()).isEqualTo(docRef("spawn"));
        assertThat(second.getCreated()).isFalse();
        assertThat(second.hasRow()).isFalse();
        assertThat(storedLong(docRef("spawn"), "score")).isEqualTo(1);
        assertThat(published).hasSize(1);
    }

    @Test
    void uniqueViolationIsAlreadyExists() {
        assertThat(create(key(), "d1", "owner", sv("x")).getCreated()).isTrue();

        StatusRuntimeException onCreate = (StatusRuntimeException) catchStatus(() -> create(key(), "d2", "owner", sv("x")));
        assertThat(onCreate.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(onCreate.getStatus().getDescription()).isEqualTo(UNIQUE_INDEX);

        assertThat(create(key(), "d3", "owner", sv("y")).getCreated()).isTrue();
        StatusRuntimeException onApply = (StatusRuntimeException) catchStatus(() -> applyNow(doc("d3"), set(OWNER, sv("x"))));
        assertThat(onApply.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS);
        assertThat(onApply.getStatus().getDescription()).isEqualTo(UNIQUE_INDEX);
        assertThat(published).hasSize(2); // only the two successful creates
    }

    static Throwable catchStatus(Runnable r) {
        try {
            r.run();
        } catch (StatusRuntimeException e) {
            return e;
        }
        throw new AssertionError("call did not fail");
    }

    @Test
    void transferInsufficientChangesNothing() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        applyNow(player(a), set(COINS, lv(10)));
        published.clear();

        TransferResponse r = transfer(key(), player(a), player(b), COINS, 11);

        assertThat(r.getTransferred()).isFalse();
        assertThat(r.getRowsList()).isEmpty();
        assertThat(coins(a)).isEqualTo(10);
        assertThat(count("SELECT count(*) FROM pantheon_player WHERE player = '" + b + "'")).isZero();
        assertThat(published).isEmpty();
    }

    @Test
    void transferUpdatesBothRows() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        applyNow(player(a), set(COINS, lv(100)));
        published.clear();
        String k = key();

        TransferResponse r = transfer(k, player(a), player(b), COINS, 40);

        assertThat(r.getTransferred()).isTrue();
        assertThat(r.getRowsList()).hasSize(2);
        assertThat(r.getRows(0).getRef().getPlayerRow().getPlayer()).isEqualTo(a.toString());
        assertThat(longOf(r.getRows(0), "coins")).isEqualTo(60);
        assertThat(longOf(r.getRows(1), "coins")).isEqualTo(40);
        assertThat(coins(a)).isEqualTo(60);
        assertThat(coins(b)).isEqualTo(40);
        assertThat(published).hasSize(2).allMatch(c -> c.getIdempotencyKey().equals(k));

        // store documents: missing ones are created, starting from the default
        TransferResponse s = transfer(key(), doc("from"), doc("to"), BANK, 30);
        assertThat(s.getTransferred()).isTrue();
        assertThat(storedLong(docRef("from"), "bank")).isEqualTo(70);
        assertThat(storedLong(docRef("to"), "bank")).isEqualTo(130);
    }

    @Test
    void deletePublishesDeleted() {
        CreateResponse created = create(key(), "d", "score", lv(1));
        published.clear();
        String k = key();

        assertThat(delete(k, "d")).isTrue();

        assertThat(published).hasSize(1);
        DocChanged c = published.getFirst();
        assertThat(c.getDeleted()).isTrue();
        assertThat(c.getDataMap()).isEmpty();
        assertThat(c.getRef()).isEqualTo(docRef("d"));
        assertThat(c.getIdempotencyKey()).isEqualTo(k);
        assertThat(c.getVersion()).isGreaterThan(created.getRow().getVersion());
        assertThat(delete(key(), "d")).isFalse();
        assertThat(published).hasSize(1);
    }

    @Test
    void storeFieldFromOtherNamespaceRejected() {
        assertThat(code(() -> applyNow(doc("d"), inc(FOREIGN, 1)))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(code(() -> transfer(key(), doc("a"), doc("b"), FOREIGN, 1))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(code(() -> transfer(key(), player(UUID.randomUUID()), player(UUID.randomUUID()), COINS, 0)))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(code(() -> applyNow(player(UUID.randomUUID()), withdraw(COINS, 0))))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void transferOppositeDirectionsNoDeadlock() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        applyNow(player(a), set(COINS, lv(1_000)));
        applyNow(player(b), set(COINS, lv(1_000)));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        long start = System.nanoTime();
        try {
            List<Future<TransferResponse>> results = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                results.add(pool.submit(() -> transfer(key(), player(a), player(b), COINS, 1)));
                results.add(pool.submit(() -> transfer(key(), player(b), player(a), COINS, 1)));
            }
            for (Future<TransferResponse> f : results) assertThat(f.get(10, TimeUnit.SECONDS).getTransferred()).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(10);
        assertThat(coins(a)).isEqualTo(1_000);
        assertThat(coins(b)).isEqualTo(1_000);
    }

    // ---------- further behaviour (cross-notes, R22, R41) ----------

    @Test
    void requestRejectionsAreInvalidArgument() {
        UUID p = UUID.randomUUID();
        // withdraw after a non-additive op on the same field
        assertThat(code(() -> applyNow(player(p), set(COINS, lv(50)), withdraw(COINS, 10))))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        // folded amounts overflow a long
        assertThat(code(() -> applyNow(player(p), inc(COINS, Long.MIN_VALUE), withdraw(COINS, 1))))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        // bigint overflow in Postgres (22003)
        applyNow(player(p), set(COINS, lv(Long.MAX_VALUE)));
        assertThat(code(() -> applyNow(player(p), inc(COINS, 1)))).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(coins(p)).isEqualTo(Long.MAX_VALUE);
        // missing key
        assertThat(code(() -> stub().apply(apply("", player(p), inc(COINS, 1))))).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void conditionOnlyRowUsesDefaultsAndIsNotCreated() throws Exception {
        UUID p = UUID.randomUUID();
        Condition rich = Condition.newBuilder().setField(COINS)
                .setPredicate(Predicate.newBuilder().setGte(lv(0))).build();
        ApplyResponse ok = stub().apply(apply(key(), player(p), add(KITS, sv("vip"))).toBuilder()
                .addConditions(rich).build());
        assertThat(ok.getApplied()).isTrue();
        assertThat(ok.getRowsList()).extracting(r -> r.getRef().getPlayerRow().getNamespace()).containsExactly("lobby");
        assertThat(count("SELECT count(*) FROM pantheon_player WHERE namespace = 'economy'")).isZero();

        Condition tooRich = rich.toBuilder().setPredicate(Predicate.newBuilder().setGt(lv(0))).build();
        ApplyResponse no = stub().apply(apply(key(), player(p), add(KITS, sv("gold"))).toBuilder()
                .addConditions(tooRich).build());
        assertThat(no.getApplied()).isFalse();
    }

    @Test
    void failedApplyIsNotStored() {
        UUID p = UUID.randomUUID();
        ApplyRequest req = apply(key(), player(p), withdraw(COINS, 5));
        assertThat(stub().apply(req).getApplied()).isFalse();
        applyNow(player(p), inc(COINS, 5));
        assertThat(stub().apply(req).getApplied()).isTrue();
        assertThat(coins(p)).isZero();
    }

    @Test
    void otherWritesAreIdempotent() {
        String createKey = key();
        CreateResponse c1 = create(createKey, "d", "score", lv(1));
        assertThat(create(createKey, "d", "score", lv(1))).isEqualTo(c1);

        String deleteKey = key();
        assertThat(delete(deleteKey, "d")).isTrue();
        create(key(), "d", "score", lv(2));
        assertThat(delete(deleteKey, "d")).isTrue(); // replay: the recreated document stays
        assertThat(storedLong(docRef("d"), "score")).isEqualTo(2);

        String expireKey = key();
        expire(expireKey, "d", System.currentTimeMillis() - 1_000);
        create(key(), "d", "score", lv(3)); // over the expired document
        expire(expireKey, "d", System.currentTimeMillis() - 1_000);
        assertThat(storedLong(docRef("d"), "score")).isEqualTo(3);

        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        applyNow(player(a), set(COINS, lv(100)));
        String transferKey = key();
        TransferResponse t1 = transfer(transferKey, player(a), player(b), COINS, 40);
        assertThat(transfer(transferKey, player(a), player(b), COINS, 40)).isEqualTo(t1);
        assertThat(coins(a)).isEqualTo(60);
        assertThat(coins(b)).isEqualTo(40);

        // a key used for a different call is rejected
        assertThat(code(() -> delete(createKey, "d"))).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void expireEmitsNoChangeAndHidesDocument() throws Exception {
        create(key(), "d", "score", lv(1));
        published.clear();
        expire(key(), "d", System.currentTimeMillis() - 1_000);
        assertThat(published).isEmpty();
        assertThat(count("SELECT count(*) FROM pantheon_doc WHERE expires_at <= now()")).isEqualTo(1);
    }

    @Test
    void writesTreatExpiredDocumentsAsAbsent() throws Exception {
        long past = System.currentTimeMillis() - 1_000;
        // create over an expired document succeeds
        create(key(), "c", "score", lv(1));
        expire(key(), "c", past);
        CreateResponse c = create(key(), "c", "score", lv(2));
        assertThat(c.getCreated()).isTrue();
        assertThat(storedLong(docRef("c"), "score")).isEqualTo(2);

        // apply starts from defaults and does not inherit the expiry
        stub().create(CreateRequest.newBuilder().setIdempotencyKey(key()).setDoc(storeDoc("a"))
                .putData("score", lv(5)).putData("bank", lv(150)).build());
        expire(key(), "a", past);
        published.clear();
        ApplyResponse r = applyNow(doc("a"), inc(SCORE, 1));
        assertThat(longOf(r.getRows(0), "score")).isEqualTo(1);
        assertThat(r.getRows(0).getDataMap()).doesNotContainKey("bank");
        assertThat(count("SELECT count(*) FROM pantheon_doc WHERE id = 'a' AND expires_at IS NULL")).isEqualTo(1);
        // the purge of the expired document is published as a deletion before the new state
        assertThat(published).extracting(DocChanged::getDeleted).containsExactly(true, false);
        assertThat(published.get(1).getVersion()).isGreaterThan(published.get(0).getVersion());

        // delete of an expired document returns false
        create(key(), "x", "score", lv(1));
        expire(key(), "x", past);
        assertThat(delete(key(), "x")).isFalse();

        // unique ignores expired documents
        create(key(), "old", "owner", sv("z"));
        expire(key(), "old", past);
        assertThat(create(key(), "new", "owner", sv("z")).getCreated()).isTrue();
    }
}

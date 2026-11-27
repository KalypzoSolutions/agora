package de.kalypzo.agora.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.grpc.AuditEndpoints;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditLocation;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.AuditViewer;
import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import de.kalypzo.agora.proto.v1.TimelineRequest;
import de.kalypzo.agora.proto.v1.TimelineResponse;
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
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-18: Timeline and pantheon:audit_view (audit_log §8, §9, §10, E31, E33, E37, R16, R26). */
class TimelineServiceTest {

    static final Instant NOW = Instant.parse("2026-11-27T12:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    static final UUID PLAYER = UUID.fromString("00000000-0000-4000-8000-000000000001");
    static final UUID OTHER = UUID.fromString("00000000-0000-4000-8000-000000000002");
    static final UUID STAFF = UUID.fromString("00000000-0000-4000-8000-0000000000aa");
    static final UUID NIL = new UUID(0, 0);
    static final AuditViewer VIEWER = AuditViewer.newBuilder()
            .setPlayer(bytes(STAFF)).setName("Notch").setVia("command").build();

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    Server server;
    ManagedChannel channel;
    AgoraGrpc.AgoraBlockingStub stub;

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
        // partitions 26th to 29th: the default range (-24 h) reaches into the 26th
        new AuditPartitions(ds, Clock.fixed(NOW.minus(Duration.ofDays(1)), ZoneOffset.UTC)).ensure(3);
    }

    @AfterAll
    static void stop() {
        ds.close();
        postgres.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("TRUNCATE pantheon_audit");
        }
        serve(new TimelineService(new AuditStore(ds), ds, CLOCK));
    }

    void serve(TimelineService timeline) throws Exception {
        if (server != null) tearDown();
        AgoraService service = new AgoraService();
        AuditEndpoints.timeline(service, timeline);
        String name = "timeline-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .intercept(new ServerInterceptor() {
                    @Override
                    public <A, B> ServerCall.Listener<A> interceptCall(
                            ServerCall<A, B> call, Metadata headers, ServerCallHandler<A, B> next) {
                        Context ctx = Context.current().withValue(NodeIdentity.CONTEXT_KEY, new NodeIdentity("lobby-1"));
                        return Contexts.interceptCall(ctx, call, headers, next);
                    }
                })
                .addService(service)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = AgoraGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
        server = null;
    }

    // ---- helpers ----

    static ByteString bytes(UUID u) {
        return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(u.getMostSignificantBits())
                .putLong(u.getLeastSignificantBits()).array());
    }

    static UUID uuid(ByteString b) {
        ByteBuffer buf = b.asReadOnlyByteBuffer();
        return new UUID(buf.getLong(), buf.getLong());
    }

    static final AtomicInteger SEQ = new AtomicInteger();

    /** A UUIDv7 at exactly this instant. */
    static UUID idAt(Instant at) {
        UUID base = UuidV7.lowerBound(at);
        return new UUID(base.getMostSignificantBits() | (SEQ.incrementAndGet() & 0xFFF),
                base.getLeastSignificantBits() | (SEQ.get() & 0xFFFFL));
    }

    static AuditEntry.Builder entry(Instant at, UUID player, String action) {
        return AuditEntry.newBuilder().setId(bytes(idAt(at))).setPlayer(bytes(player)).setAction(action);
    }

    static void insert(AuditEntry.Builder... entries) throws SQLException {
        List<StoredAuditEntry> rows = new ArrayList<>();
        for (AuditEntry.Builder e : entries) {
            rows.add(StoredAuditEntry.newBuilder().setEntry(e).setServer("survival-1").build());
        }
        new AuditStore(ds).insert(rows);
    }

    static TimelineRequest.Builder request(UUID player) {
        return TimelineRequest.newBuilder().setPlayer(bytes(player)).setViewer(VIEWER);
    }

    static List<UUID> ids(TimelineResponse r) {
        return r.getEntriesList().stream().map(s -> uuid(s.getEntry().getId())).toList();
    }

    static List<AuditEntry> auditViews() throws SQLException {
        List<AuditEntry> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT id FROM pantheon_audit WHERE action = 'pantheon:audit_view' ORDER BY id")) {
            while (rs.next()) out.add(AuditEntry.newBuilder().setId(bytes(rs.getObject(1, UUID.class))).build());
        }
        return out;
    }

    static int count(String where) throws SQLException {
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM pantheon_audit WHERE " + where)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    static long millis(Instant t) {
        return t.toEpochMilli();
    }

    static void assertStatus(Runnable call, Status.Code code) {
        assertThatThrownBy(call::run)
                .isInstanceOf(StatusRuntimeException.class)
                .extracting(t -> ((StatusRuntimeException) t).getStatus().getCode())
                .isEqualTo(code);
    }

    // ---- tests ----

    @Test
    void playerAndTargetEntriesNewestFirst() throws Exception {
        AuditEntry.Builder own = entry(NOW.minusSeconds(30), PLAYER, "shop:buy")
                .setLocation(AuditLocation.newBuilder().setWorld("world").setX(12.5).setY(64).setZ(-3))
                .putWith("item", AuditValue.newBuilder().setS("diamond_sword").build())
                .putWith("amount", AuditValue.newBuilder().setL(250).build())
                .putWith("price", AuditValue.newBuilder().setD(1.5).build())
                .putWith("big", AuditValue.newBuilder().setD(1.0E7).build())
                .putWith("ok", AuditValue.newBuilder().setB(true).build());
        AuditEntry.Builder asTarget = entry(NOW.minusSeconds(20), OTHER, "pantheon:private_message")
                .setTarget(bytes(PLAYER));
        AuditEntry.Builder newest = entry(NOW.minusSeconds(10), PLAYER, "pantheon:chat");
        AuditEntry.Builder foreign = entry(NOW.minusSeconds(15), OTHER, "pantheon:chat");
        insert(own, asTarget, newest, foreign);

        TimelineResponse r = stub.timeline(request(PLAYER).build());

        assertThat(ids(r)).containsExactly(uuid(newest.getId()), uuid(asTarget.getId()), uuid(own.getId()));
        assertThat(r.getEntries(2).getEntry()).isEqualTo(own.build());
        assertThat(r.getEntries(2).getServer()).isEqualTo("survival-1");
        assertThat(r.getEntries(1).getEntry()).isEqualTo(asTarget.build());
        assertThat(r.hasCursor()).isFalse();
    }

    @Test
    void actionFilterExactAndNamespace() throws Exception {
        AuditEntry.Builder buy = entry(NOW.minusSeconds(50), PLAYER, "shop:buy");
        AuditEntry.Builder open = entry(NOW.minusSeconds(40), PLAYER, "shop:open");
        AuditEntry.Builder chat = entry(NOW.minusSeconds(30), PLAYER, "pantheon:chat");
        AuditEntry.Builder lookalike = entry(NOW.minusSeconds(20), PLAYER, "shopx:buy");
        AuditEntry.Builder underscore = entry(NOW.minusSeconds(10), PLAYER, "myxns:a");
        AuditEntry.Builder ns = entry(NOW.minusSeconds(5), PLAYER, "my_ns:a");
        insert(buy, open, chat, lookalike, underscore, ns);

        assertThat(ids(stub.timeline(request(PLAYER).addActions("shop:buy").build())))
                .containsExactly(uuid(buy.getId()));
        assertThat(ids(stub.timeline(request(PLAYER).addActions("shop:*").build())))
                .containsExactly(uuid(open.getId()), uuid(buy.getId()));
        assertThat(ids(stub.timeline(request(PLAYER).addActions("shop:*").addActions("pantheon:chat").build())))
                .containsExactly(uuid(chat.getId()), uuid(open.getId()), uuid(buy.getId()));
        // '_' is no LIKE wildcard
        assertThat(ids(stub.timeline(request(PLAYER).addActions("my_ns:*").build())))
                .containsExactly(uuid(ns.getId()));

        for (String bad : List.of("shop", "Shop:buy", "shop:*x", "*", "shop:b%", "shop:", ":buy", "sh*:buy")) {
            assertStatus(() -> stub.timeline(request(PLAYER).addActions(bad).build()), Status.Code.INVALID_ARGUMENT);
        }
    }

    @Test
    void traceFilter() throws Exception {
        UUID trace = idAt(NOW.minusSeconds(60));
        AuditEntry.Builder a = entry(NOW.minusSeconds(50), PLAYER, "shop:open").setTrace(bytes(trace));
        AuditEntry.Builder b = entry(NOW.minusSeconds(40), PLAYER, "shop:buy").setTrace(bytes(trace));
        AuditEntry.Builder other = entry(NOW.minusSeconds(30), PLAYER, "shop:buy").setTrace(bytes(idAt(NOW)));
        AuditEntry.Builder none = entry(NOW.minusSeconds(20), PLAYER, "pantheon:chat");
        insert(a, b, other, none);

        TimelineResponse r = stub.timeline(request(PLAYER).setTrace(bytes(trace)).build());

        assertThat(ids(r)).containsExactly(uuid(b.getId()), uuid(a.getId()));
    }

    @Test
    void cursorPagesWithoutGapsOrDuplicates() throws Exception {
        List<AuditEntry.Builder> all = new ArrayList<>();
        for (int i = 0; i < 1234; i++) {
            // several entries share a millisecond: the cursor must not skip or repeat any of them
            all.add(entry(NOW.minusMillis(10_000 - (i / 3)), PLAYER, "shop:buy"));
        }
        insert(all.toArray(AuditEntry.Builder[]::new));
        List<UUID> expected = new ArrayList<>(all.stream().map(e -> uuid(e.getId())).toList());
        expected.sort(java.util.Comparator.reverseOrder());

        List<UUID> seen = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        TimelineResponse page = stub.timeline(request(PLAYER).setLimit(500).build());
        while (true) {
            seen.addAll(ids(page));
            sizes.add(page.getEntriesCount());
            if (!page.hasCursor()) break;
            page = stub.timeline(request(PLAYER).setLimit(500).setCursor(page.getCursor()).build());
        }

        assertThat(sizes).containsExactly(500, 500, 234);
        assertThat(seen).hasSize(1234).doesNotHaveDuplicates();
        // UUIDs compare as signed longs in Java; the v7 prefix keeps the first long positive, so the order is equal
        assertThat(seen).isEqualTo(expected);
    }

    @Test
    void limitClampedTo500() throws Exception {
        List<AuditEntry.Builder> all = new ArrayList<>();
        for (int i = 0; i < 600; i++) all.add(entry(NOW.minusSeconds(600 - i), PLAYER, "shop:buy"));
        insert(all.toArray(AuditEntry.Builder[]::new));

        TimelineResponse clamped = stub.timeline(request(PLAYER).setLimit(1000).build());
        assertThat(clamped.getEntriesCount()).isEqualTo(500);
        assertThat(clamped.hasCursor()).isTrue();
        assertThat(uuid(clamped.getCursor())).isEqualTo(uuid(clamped.getEntries(499).getEntry().getId()));

        TimelineResponse huge = stub.timeline(request(PLAYER).setLimit(-1).build()); // uint32 max
        assertThat(huge.getEntriesCount()).isEqualTo(500);

        TimelineResponse byDefault = stub.timeline(request(PLAYER).build());
        assertThat(byDefault.getEntriesCount()).isEqualTo(100);
        assertThat(byDefault.hasCursor()).isTrue();
    }

    @Test
    void defaultRangeIs24h() throws Exception {
        AuditEntry.Builder tooOld = entry(NOW.minus(Duration.ofHours(24)).minusMillis(1), PLAYER, "shop:buy");
        AuditEntry.Builder oldest = entry(NOW.minus(Duration.ofHours(24)), PLAYER, "shop:buy");
        AuditEntry.Builder newest = entry(NOW.minusMillis(1), PLAYER, "shop:buy");
        insert(tooOld, oldest, newest);

        TimelineResponse r = stub.timeline(request(PLAYER).build());
        assertThat(ids(r)).containsExactly(uuid(newest.getId()), uuid(oldest.getId()));

        // only `to` set: from = to - 24 h (= tooOld's instant); `to` is exclusive (newest is not included)
        TimelineResponse onlyTo = stub.timeline(request(PLAYER).setToMillis(millis(NOW.minusMillis(1))).build());
        assertThat(ids(onlyTo)).containsExactly(uuid(oldest.getId()), uuid(tooOld.getId()));
        // only `from` set: to = now
        TimelineResponse onlyFrom = stub.timeline(request(PLAYER)
                .setFromMillis(millis(NOW.minus(Duration.ofHours(24)).minusMillis(1))).build());
        assertThat(ids(onlyFrom)).containsExactly(uuid(newest.getId()), uuid(oldest.getId()), uuid(tooOld.getId()));
    }

    @Test
    void missingViewerRejected() throws Exception {
        assertStatus(() -> stub.timeline(TimelineRequest.newBuilder().setPlayer(bytes(PLAYER)).build()),
                Status.Code.INVALID_ARGUMENT);
        assertThat(count("true")).isZero();
    }

    @Test
    void emptyViewerNameRejected() throws Exception {
        assertStatus(() -> stub.timeline(TimelineRequest.newBuilder().setPlayer(bytes(PLAYER))
                .setViewer(AuditViewer.newBuilder().setPlayer(bytes(STAFF)).setVia("web")).build()),
                Status.Code.INVALID_ARGUMENT);
        assertThat(count("true")).isZero();
    }

    @Test
    void invalidUuidsRejected() throws Exception {
        assertStatus(() -> stub.timeline(request(PLAYER).setPlayer(ByteString.copyFrom(new byte[8])).build()),
                Status.Code.INVALID_ARGUMENT);
        assertStatus(() -> stub.timeline(request(PLAYER).setCursor(ByteString.copyFrom(new byte[15])).build()),
                Status.Code.INVALID_ARGUMENT);
        assertStatus(() -> stub.timeline(request(PLAYER).setTrace(ByteString.copyFrom(new byte[17])).build()),
                Status.Code.INVALID_ARGUMENT);
        assertStatus(() -> stub.timeline(request(PLAYER)
                .setViewer(VIEWER.toBuilder().setPlayer(ByteString.copyFrom(new byte[4]))).build()),
                Status.Code.INVALID_ARGUMENT);
        assertStatus(() -> stub.timeline(request(PLAYER).setFromMillis(-5).build()), Status.Code.INVALID_ARGUMENT);
        assertThat(count("true")).isZero();
    }

    @Test
    void firstPageWritesAuditView() throws Exception {
        UUID trace = idAt(NOW.minusSeconds(100));
        AuditEntry.Builder e = entry(NOW.minusSeconds(10), PLAYER, "shop:buy").setTrace(bytes(trace));
        insert(e);
        long from = millis(NOW.minus(Duration.ofHours(6)));
        long to = millis(NOW.plusSeconds(60));

        TimelineResponse r = stub.timeline(request(PLAYER).setFromMillis(from).setToMillis(to)
                .addActions("shop:buy").addActions("shop:*").setTrace(bytes(trace)).build());

        // R16: the view written for this request is not on its own page
        assertThat(ids(r)).containsExactly(uuid(e.getId()));

        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT id, player, target, trace, server, world, with_data::text"
                        + " FROM pantheon_audit WHERE action = 'pantheon:audit_view'")) {
            assertThat(rs.next()).isTrue();
            UUID id = rs.getObject("id", UUID.class);
            assertThat(UuidV7.timestamp(id)).isEqualTo(NOW);
            assertThat(rs.getObject("player", UUID.class)).isEqualTo(STAFF);
            assertThat(rs.getObject("target", UUID.class)).isEqualTo(PLAYER);
            assertThat(rs.getObject("trace", UUID.class)).isNull();
            assertThat(rs.getString("server")).isEqualTo("lobby-1");
            assertThat(rs.getString("world")).isNull();
            assertThat(rs.getString("with_data")).isEqualTo("{\"to\": " + to + ", \"via\": \"command\", \"from\": "
                    + from + ", \"trace\": \"" + trace + "\", \"actions\": \"shop:buy,shop:*\", \"viewer_name\": \"Notch\"}");
            assertThat(rs.next()).isFalse();
        }

        // a later query (without action filter) sees the view in the target's timeline
        TimelineResponse later = stub.timeline(request(PLAYER).setToMillis(to).build());
        assertThat(later.getEntries(0).getEntry().getAction()).isEqualTo("pantheon:audit_view");
        assertThat(later.getEntries(0).getServer()).isEqualTo("lobby-1");
        assertThat(later.getEntries(0).getEntry().getWithMap().get("viewer_name").getS()).isEqualTo("Notch");
        assertThat(later.getEntries(0).getEntry().getWithMap().get("from").getL()).isEqualTo(from);
    }

    @Test
    void firstPageWithoutFiltersWritesEffectiveRange() throws Exception {
        stub.timeline(request(PLAYER).build());

        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT with_data::text FROM pantheon_audit"
                        + " WHERE action = 'pantheon:audit_view'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("{\"to\": " + millis(NOW) + ", \"via\": \"command\", \"from\": "
                    + millis(NOW.minus(Duration.ofHours(24))) + ", \"viewer_name\": \"Notch\"}");
        }
    }

    @Test
    void nextPageWritesNoAuditView() throws Exception {
        List<AuditEntry.Builder> all = new ArrayList<>();
        for (int i = 0; i < 5; i++) all.add(entry(NOW.minusSeconds(60 - i), PLAYER, "shop:buy"));
        insert(all.toArray(AuditEntry.Builder[]::new));

        TimelineResponse first = stub.timeline(request(PLAYER).setLimit(2).build());
        assertThat(auditViews()).hasSize(1);
        TimelineResponse second = stub.timeline(request(PLAYER).setLimit(2).setCursor(first.getCursor()).build());
        TimelineResponse third = stub.timeline(request(PLAYER).setLimit(2).setCursor(second.getCursor()).build());

        assertThat(auditViews()).hasSize(1);
        assertThat(first.getEntriesCount() + second.getEntriesCount() + third.getEntriesCount()).isEqualTo(5);
        assertThat(third.hasCursor()).isFalse();
    }

    @Test
    void viewerWithoutPlayerUsesNilUuid() throws Exception {
        stub.timeline(TimelineRequest.newBuilder().setPlayer(bytes(PLAYER))
                .setViewer(AuditViewer.newBuilder().setName("console").setVia("command")).build());

        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT player, target, with_data->>'viewer_name' FROM pantheon_audit"
                        + " WHERE action = 'pantheon:audit_view'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject(1, UUID.class)).isEqualTo(NIL);
            assertThat(rs.getObject(2, UUID.class)).isEqualTo(PLAYER);
            assertThat(rs.getString(3)).isEqualTo("console");
        }
    }

    @Test
    void auditViewWriteFailureUnavailableAndNothingRead() throws Exception {
        insert(entry(NOW.minusSeconds(10), PLAYER, "shop:buy"));
        AuditStore failing = new AuditStore(ds) {
            @Override
            public void insert(List<StoredAuditEntry> rows) throws SQLException {
                throw new SQLException("down");
            }
        };
        AtomicInteger reads = new AtomicInteger();
        DataSource counting = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {DataSource.class}, (p, m, args) -> {
                    if (m.getName().equals("getConnection")) reads.incrementAndGet();
                    return m.invoke(ds, args);
                });
        serve(new TimelineService(failing, counting, CLOCK));

        assertStatus(() -> stub.timeline(request(PLAYER).build()), Status.Code.UNAVAILABLE);
        assertThat(reads).hasValue(0);
        // a next page writes nothing, so it can still be read
        assertThat(stub.timeline(request(PLAYER).setCursor(bytes(idAt(NOW))).build()).getEntriesCount()).isEqualTo(1);
    }

    @Test
    void acrossPartitionBoundary() throws Exception {
        Instant beforeMidnight = Instant.parse("2026-11-26T23:59:59.999Z");
        Instant midnight = Instant.parse("2026-11-27T00:00:00.000Z");
        AuditEntry.Builder a = entry(beforeMidnight.minusMillis(1), PLAYER, "shop:buy");
        AuditEntry.Builder b = entry(beforeMidnight, PLAYER, "shop:buy");
        AuditEntry.Builder c = entry(beforeMidnight, PLAYER, "shop:buy");
        AuditEntry.Builder d = entry(midnight, PLAYER, "shop:buy");
        AuditEntry.Builder e = entry(midnight, PLAYER, "shop:buy");
        AuditEntry.Builder f = entry(midnight.plusMillis(1), PLAYER, "shop:buy");
        insert(a, b, c, d, e, f);
        assertThat(count("tableoid = 'pantheon_audit_20261126'::regclass")).isEqualTo(3);
        assertThat(count("tableoid = 'pantheon_audit_20261127'::regclass")).isEqualTo(3);
        List<UUID> expected = List.of(uuid(f.getId()), uuid(e.getId()), uuid(d.getId()),
                uuid(c.getId()), uuid(b.getId()), uuid(a.getId()));

        // one page over both partitions
        assertThat(ids(stub.timeline(request(PLAYER).build()))).isEqualTo(expected);

        // paging by 1 and by 2 (page edge exactly at midnight) loses nothing
        for (int limit : List.of(1, 2, 4)) {
            List<UUID> seen = new ArrayList<>();
            TimelineResponse page = stub.timeline(request(PLAYER).setLimit(limit)
                    .setToMillis(millis(midnight.plusMillis(2))).build());
            while (true) {
                seen.addAll(ids(page));
                if (!page.hasCursor()) break;
                page = stub.timeline(request(PLAYER).setLimit(limit).setToMillis(millis(midnight.plusMillis(2)))
                        .setCursor(page.getCursor()).build());
            }
            assertThat(seen).as("limit " + limit).isEqualTo(expected);
        }

        // a range ending exactly at midnight (exclusive) only reads the 26th
        assertThat(ids(stub.timeline(request(PLAYER).setFromMillis(millis(beforeMidnight.minusMillis(1)))
                .setToMillis(millis(midnight)).build())))
                .containsExactly(uuid(c.getId()), uuid(b.getId()), uuid(a.getId()));
    }

    static final Pattern PARTITION = Pattern.compile("pantheon_audit_(\\d{8})");

    static Set<String> partitionsInPlan(TimelineService.Query q) throws SQLException {
        Set<String> found = new HashSet<>();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("EXPLAIN " + q.sql())) {
            q.bind(c, ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Matcher m = PARTITION.matcher(rs.getString(1));
                    while (m.find()) found.add(m.group(1));
                }
            }
        }
        return found;
    }

    @Test
    void onlyPartitionsOfRangeScanned() throws Exception {
        TimelineService svc = new TimelineService(new AuditStore(ds), ds, CLOCK);
        long from = millis(Instant.parse("2026-11-27T00:00:00Z"));
        long to = millis(Instant.parse("2026-11-28T00:00:00Z"));

        TimelineService.Query day = svc.query(request(PLAYER).setFromMillis(from).setToMillis(to)
                .addActions("shop:*").build(), null);
        assertThat(day.sql()).contains("'" + UuidV7.lowerBound(Instant.ofEpochMilli(from)) + "'");
        assertThat(partitionsInPlan(day)).containsExactly("20261127");

        // with cursor and trace still one partition; the default range (26th 12:00 to 27th 12:00) covers two
        TimelineService.Query paged = svc.query(request(PLAYER).setFromMillis(from).setToMillis(to)
                .setCursor(bytes(idAt(NOW))).setTrace(bytes(idAt(NOW))).build(), idAt(NOW));
        assertThat(partitionsInPlan(paged)).containsExactly("20261127");
        assertThat(partitionsInPlan(svc.query(request(PLAYER).build(), null)))
                .containsExactlyInAnyOrder("20261126", "20261127");
    }
}

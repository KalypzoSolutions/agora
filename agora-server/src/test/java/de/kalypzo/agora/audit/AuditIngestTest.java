package de.kalypzo.agora.audit;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.protobuf.ByteString;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.grpc.AuditEndpoints;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.AuditBundle;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditLocation;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-17: AppendAudit (audit_log §7, §8, E29, E30, E34). */
class AuditIngestTest {

    static final Instant NOW = Instant.parse("2026-11-27T12:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    static final UUID PLAYER = UUID.fromString("00000000-0000-4000-8000-000000000001");

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    Server server;
    ManagedChannel channel;
    AgoraGrpc.AgoraBlockingStub stub;
    ListAppender<ILoggingEvent> logs;
    ch.qos.logback.classic.Logger ingestLogger;

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
        // -24 h reaches into the previous day: partitions 26th to 28th (+1 h stays on the 27th)
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
        AgoraService service = new AgoraService();
        AuditEndpoints.register(service, new AuditIngest(new AuditStore(ds), CLOCK));
        String name = "audit-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .maxInboundMessageSize(16 * 1024 * 1024)
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
        ingestLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AuditIngest.class);
        logs = new ListAppender<>();
        logs.start();
        ingestLogger.addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        ingestLogger.detachAppender(logs);
        channel.shutdownNow();
        server.shutdownNow();
    }

    static ByteString bytes(UUID u) {
        return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(u.getMostSignificantBits())
                .putLong(u.getLeastSignificantBits()).array());
    }

    static final java.util.concurrent.atomic.AtomicInteger SEQ = new java.util.concurrent.atomic.AtomicInteger();

    /** A UUIDv7 at exactly this instant (the shared generator would be clamped by later instants). */
    static UUID idAt(Instant at) {
        UUID base = UuidV7.lowerBound(at);
        return new UUID(base.getMostSignificantBits() | (SEQ.incrementAndGet() & 0xFFF), base.getLeastSignificantBits());
    }

    static AuditEntry.Builder entry(Instant at) {
        return AuditEntry.newBuilder()
                .setId(bytes(idAt(at)))
                .setPlayer(bytes(PLAYER))
                .setAction("shop:buy");
    }

    static List<UUID> ids() throws Exception {
        List<UUID> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT id FROM pantheon_audit ORDER BY id")) {
            while (rs.next()) out.add(rs.getObject(1, UUID.class));
        }
        return out;
    }

    static UUID idOf(AuditEntry e) {
        ByteBuffer b = e.getId().asReadOnlyByteBuffer();
        return new UUID(b.getLong(), b.getLong());
    }

    @Test
    void serverComesFromAuthenticatedNode() throws Exception {
        AuditEntry e = entry(NOW)
                .setLocation(AuditLocation.newBuilder().setWorld("w").setX(1.5).setY(2).setZ(3))
                .putWith("price", AuditValue.newBuilder().setL(42).build())
                .putWith("item", AuditValue.newBuilder().setS("diamond").build())
                .build();
        stub.appendAudit(AuditBundle.newBuilder().addEntries(e).build());
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT server, action, world, x, with_data->>'item',"
                        + " (with_data->>'price')::bigint, jsonb_typeof(with_data->'price') FROM pantheon_audit")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("lobby-1");
            assertThat(rs.getString(2)).isEqualTo("shop:buy");
            assertThat(rs.getString(3)).isEqualTo("w");
            assertThat(rs.getDouble(4)).isEqualTo(1.5);
            assertThat(rs.getString(5)).isEqualTo("diamond");
            assertThat(rs.getLong(6)).isEqualTo(42);
            assertThat(rs.getString(7)).isEqualTo("number");
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void duplicateIdsInsertedOnce() throws Exception {
        AuditBundle bundle = AuditBundle.newBuilder()
                .addEntries(entry(NOW)).addEntries(entry(NOW.plusMillis(5))).build();
        stub.appendAudit(bundle);
        stub.appendAudit(bundle);
        assertThat(ids()).hasSize(2).doesNotHaveDuplicates();
    }

    @Test
    void entryOutsideWindowDroppedOthersStored() throws Exception {
        AuditEntry tooOld = entry(NOW.minus(Duration.ofHours(24)).minusSeconds(1)).build();
        AuditEntry tooNew = entry(NOW.plus(Duration.ofHours(1)).plusSeconds(1)).build();
        AuditEntry oldOk = entry(NOW.minus(Duration.ofMinutes(23 * 60 + 59))).build();
        AuditEntry now = entry(NOW).build();
        stub.appendAudit(AuditBundle.newBuilder()
                .addEntries(tooOld).addEntries(oldOk).addEntries(now).addEntries(tooNew).build());
        assertThat(ids()).containsExactly(idOf(oldOk), idOf(now));
    }

    @Test
    void invalidEntryDroppedBundleAccepted() throws Exception {
        AuditEntry shortPlayer = entry(NOW).setPlayer(ByteString.copyFrom(new byte[15])).build();
        AuditEntry badAction = entry(NOW.plusMillis(1)).setAction("Shop:Buy").build();
        AuditEntry good = entry(NOW.plusMillis(2)).build();
        stub.appendAudit(AuditBundle.newBuilder()
                .addEntries(shortPlayer).addEntries(badAction).addEntries(good).build());
        assertThat(ids()).containsExactly(idOf(good));
    }

    @Test
    void droppedIsLoggedWithNode() {
        stub.appendAudit(AuditBundle.newBuilder().setDropped(3)
                .addEntries(entry(NOW).setAction("Bad")).addEntries(entry(NOW.plusMillis(1))).build());
        assertThat(logs.list).filteredOn(ev -> ev.getLevel() == Level.WARN)
                .extracting(ILoggingEvent::getFormattedMessage)
                .containsExactly("Audit: 4 Einträge von lobby-1 verworfen");
    }

    @Test
    void cleanBundleLogsNoWarning() {
        stub.appendAudit(AuditBundle.newBuilder().addEntries(entry(NOW)).build());
        assertThat(logs.list).isEmpty();
    }

    @Test
    void extraInvalidEntriesDroppedOthersStored() throws Exception {
        AuditEntry nul = entry(NOW).putWith("k", AuditValue.newBuilder().setS("a\u0000b").build()).build();
        AuditEntry nan = entry(NOW.plusMillis(1))
                .putWith("k", AuditValue.newBuilder().setD(Double.NaN).build()).build();
        AuditEntry inf = entry(NOW.plusMillis(2))
                .putWith("k", AuditValue.newBuilder().setD(Double.POSITIVE_INFINITY).build()).build();
        AuditEntry badTarget = entry(NOW.plusMillis(3)).setTarget(ByteString.copyFrom(new byte[4])).build();
        AuditEntry badTrace = entry(NOW.plusMillis(4)).setTrace(ByteString.copyFrom(new byte[17])).build();
        AuditEntry badId = entry(NOW.plusMillis(5)).setId(ByteString.copyFrom(new byte[8])).build();
        AuditEntry good = entry(NOW.plusMillis(6)).putWith("k", AuditValue.newBuilder().setD(1.5).build()).build();
        stub.appendAudit(AuditBundle.newBuilder().addEntries(nul).addEntries(nan).addEntries(inf)
                .addEntries(badTarget).addEntries(badTrace).addEntries(badId).addEntries(good).build());
        assertThat(ids()).containsExactly(idOf(good));
        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                .containsExactly("Audit: 6 Einträge von lobby-1 verworfen");
    }

    @Test
    void databaseFailureAnswersUnavailable() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(postgres.getJdbcUrl());
        cfg.setUsername(postgres.getUsername());
        cfg.setPassword(postgres.getPassword());
        HikariDataSource dead = new HikariDataSource(cfg);
        dead.close();
        AgoraService service = new AgoraService();
        AuditEndpoints.register(service, new AuditIngest(new AuditStore(dead), CLOCK));
        String name = "audit-dead-" + UUID.randomUUID();
        Server s = null;
        ManagedChannel ch = null;
        try {
            s = InProcessServerBuilder.forName(name).directExecutor()
                    .intercept(new ServerInterceptor() {
                        @Override
                        public <A, B> ServerCall.Listener<A> interceptCall(
                                ServerCall<A, B> call, Metadata headers, ServerCallHandler<A, B> next) {
                            Context ctx = Context.current()
                                    .withValue(NodeIdentity.CONTEXT_KEY, new NodeIdentity("lobby-1"));
                            return Contexts.interceptCall(ctx, call, headers, next);
                        }
                    }).addService(service).build().start();
            ch = InProcessChannelBuilder.forName(name).directExecutor().build();
            var deadStub = AgoraGrpc.newBlockingStub(ch);
            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () -> deadStub.appendAudit(AuditBundle.newBuilder().addEntries(entry(NOW)).build()))
                    .isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                            e -> assertThat(e.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.UNAVAILABLE));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        } finally {
            if (ch != null) ch.shutdownNow();
            if (s != null) s.shutdownNow();
        }
    }
}

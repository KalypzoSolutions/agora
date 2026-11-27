package de.kalypzo.agora.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.PlayerLeft;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A-19: session entries from the presence (audit_log §5, E31, R2). */
class SessionAuditorTest {

    static final Instant NOW = Instant.parse("2026-11-27T12:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    static final UUID STEVE = UUID.fromString("00000000-0000-4000-8000-000000000001");
    static final UUID ALEX = UUID.fromString("00000000-0000-4000-8000-000000000002");

    final List<StoredAuditEntry> written = new CopyOnWriteArrayList<>();
    volatile CountDownLatch dbGate; // when set, insert blocks until it opens
    final AuditStore store = new AuditStore(null) {
        @Override
        public void insert(List<StoredAuditEntry> rows) throws SQLException {
            CountDownLatch gate = dbGate;
            if (gate != null) {
                try {
                    gate.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            written.addAll(rows);
        }
    };
    ScheduledExecutorService scheduler;
    AuditSink sink;
    NodeRegistry registry;
    PresenceService presence;
    final java.util.concurrent.atomic.AtomicReference<Duration> offset =
            new java.util.concurrent.atomic.AtomicReference<>(Duration.ZERO);

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        sink = new AuditSink(store, scheduler);
        registry = new NodeRegistry();
        Clock moving = new Clock() {
            public ZoneOffset getZone() { return ZoneOffset.UTC; }
            public Clock withZone(java.time.ZoneId z) { return this; }
            public Instant instant() { return NOW.plus(offset.get()); }
        };
        presence = new PresenceService(registry, moving, Duration.ofMillis(100), new SessionAuditor(sink, CLOCK));
        presence.install(new de.kalypzo.agora.node.NodeMessageDispatcher()); // node-gone
    }

    @AfterEach
    void tearDown() {
        if (dbGate != null) dbGate.countDown();
        scheduler.shutdownNow();
        registry.close(Duration.ofSeconds(2));
    }

    NodeSession proxy(String id) {
        NodeSession s = new NodeSession(id, NodeKind.VELOCITY, NOW, new StreamObserver<ServiceMessage>() {
            public void onNext(ServiceMessage v) {}
            public void onError(Throwable t) {}
            public void onCompleted() {}
        });
        registry.register(s);
        return s;
    }

    static PresenceEntry entry(UUID player, String name) {
        return PresenceEntry.newBuilder().setPlayer(player.toString()).setName(name).setLocale("de_DE").build();
    }

    void claim(NodeSession proxy, UUID player) throws Exception {
        assertThat(presence.claim(proxy, entry(player, "P-" + player.getLeastSignificantBits()))
                .get(5, TimeUnit.SECONDS)).isTrue();
    }

    void switched(NodeSession proxy, UUID player, String server) {
        presence.onPlayerSwitched(proxy, PlayerSwitched.newBuilder().setPlayer(player.toString()).setServer(server).build());
        presence.barrier(player).join();
    }

    void left(NodeSession proxy, UUID player) {
        presence.onPlayerLeft(proxy, PlayerLeft.newBuilder().setPlayer(player.toString()).build());
        presence.barrier(player).join();
    }

    List<StoredAuditEntry> flushed() {
        sink.flush();
        return List.copyOf(written);
    }

    static Map<String, AuditValue> with(StoredAuditEntry e) {
        return e.getEntry().getWithMap();
    }

    static UUID id(StoredAuditEntry e) {
        return AuditStore.uuid(e.getEntry().getId());
    }

    @Test
    void firstSwitchWritesJoinWithTo() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        claim(p1, STEVE);
        assertThat(flushed()).isEmpty(); // a claim alone is no join
        switched(p1, STEVE, "lobby-01");

        assertThat(flushed()).singleElement().satisfies(e -> {
            assertThat(e.getEntry().getAction()).isEqualTo("pantheon:join");
            assertThat(AuditStore.uuid(e.getEntry().getPlayer())).isEqualTo(STEVE);
            assertThat(with(e)).containsOnlyKeys("to");
            assertThat(with(e).get("to").getS()).isEqualTo("lobby-01");
            assertThat(e.getEntry().hasLocation()).isFalse();
            assertThat(e.getEntry().hasTarget()).isFalse();
            assertThat(e.getEntry().hasTrace()).isFalse();
            assertThat(UuidV7.timestamp(id(e))).isEqualTo(NOW); // own generator with the injected clock
            assertThat(id(e).version()).isEqualTo(7);
        });
    }

    @Test
    void laterSwitchWritesSwitchFromTo() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        claim(p1, STEVE);
        switched(p1, STEVE, "lobby-01");
        switched(p1, STEVE, "survival-01");

        List<StoredAuditEntry> rows = flushed();
        assertThat(rows).extracting(e -> e.getEntry().getAction()).containsExactly("pantheon:join", "pantheon:switch");
        StoredAuditEntry sw = rows.get(1);
        assertThat(with(sw)).containsOnlyKeys("from", "to");
        assertThat(with(sw).get("from").getS()).isEqualTo("lobby-01");
        assertThat(with(sw).get("to").getS()).isEqualTo("survival-01");
        assertThat(id(rows.get(1))).isGreaterThan(id(rows.get(0))); // monotonic ids
    }

    @Test
    void leftWritesQuitLeft() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        claim(p1, STEVE);
        switched(p1, STEVE, "lobby-01");
        left(p1, STEVE);

        assertThat(flushed()).last().satisfies(e -> {
            assertThat(e.getEntry().getAction()).isEqualTo("pantheon:quit");
            assertThat(with(e)).containsOnlyKeys("from", "reason");
            assertThat(with(e).get("from").getS()).isEqualTo("lobby-01");
            assertThat(with(e).get("reason").getS()).isEqualTo("left");
        });
    }

    @Test
    void doubleLoginWritesQuitReplacedForOldEntry() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        NodeSession p2 = proxy("proxy-02");
        claim(p1, STEVE);
        switched(p1, STEVE, "lobby-01");
        claim(p2, STEVE); // the silent old proxy times out, the old entry is replaced

        List<StoredAuditEntry> rows = flushed();
        assertThat(rows).extracting(e -> e.getEntry().getAction()).containsExactly("pantheon:join", "pantheon:quit");
        StoredAuditEntry quit = rows.get(1);
        assertThat(quit.getServer()).isEqualTo("proxy-01"); // the old entry's proxy
        assertThat(with(quit).get("from").getS()).isEqualTo("lobby-01");
        assertThat(with(quit).get("reason").getS()).isEqualTo("replaced");
        // the new entry has no server yet: no join before its first switch
        switched(p2, STEVE, "lobby-02");
        assertThat(flushed()).last().satisfies(e -> {
            assertThat(e.getEntry().getAction()).isEqualTo("pantheon:join");
            assertThat(e.getServer()).isEqualTo("proxy-02");
        });
    }

    @Test
    void proxyGoneWritesQuitProxyLost() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        claim(p1, STEVE);
        switched(p1, STEVE, "lobby-01");
        p1.peerClosed(Status.CANCELLED);

        await().atMost(Duration.ofSeconds(5)).until(() -> flushed().size() == 2);
        StoredAuditEntry quit = written.get(1);
        assertThat(quit.getEntry().getAction()).isEqualTo("pantheon:quit");
        assertThat(quit.getServer()).isEqualTo("proxy-01");
        assertThat(with(quit).get("from").getS()).isEqualTo("lobby-01");
        assertThat(with(quit).get("reason").getS()).isEqualTo("proxy_lost");
    }

    @Test
    void proxyGoneOfSupersededSessionWritesNothing() throws Exception {
        NodeSession old = proxy("proxy-01");
        claim(old, STEVE);
        switched(old, STEVE, "lobby-01");
        proxy("proxy-01"); // reconnect: old is superseded (R2)
        old.peerClosed(Status.CANCELLED);
        registry.serialize("proxy-01", () -> {}).get(5, TimeUnit.SECONDS);

        assertThat(flushed()).extracting(e -> e.getEntry().getAction()).containsExactly("pantheon:join");
    }

    @Test
    void nothingAfterShutdown() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        claim(p1, STEVE);
        switched(p1, STEVE, "lobby-01");
        registry.shutdown(Status.UNAVAILABLE);
        left(p1, STEVE);
        registry.serialize("proxy-01", () -> {}).get(5, TimeUnit.SECONDS);

        assertThat(flushed()).extracting(e -> e.getEntry().getAction()).containsExactly("pantheon:join");
    }

    @Test
    void leaveWithoutServerWritesNothing() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        NodeSession p2 = proxy("proxy-02");
        claim(p1, STEVE);
        left(p1, STEVE); // never reached a server
        claim(p1, ALEX);
        claim(p2, ALEX); // replaced while still without server
        PresenceEntry bob = entry(UUID.fromString("00000000-0000-4000-8000-000000000003"), "Bob");
        presence.claim(p2, bob).get(5, TimeUnit.SECONDS);
        p2.peerClosed(Status.CANCELLED); // proxy lost while still without server
        registry.serialize("proxy-02", () -> {}).get(5, TimeUnit.SECONDS);

        assertThat(flushed()).isEmpty();
    }

    @Test
    void reconciliationWritesNothing() throws Exception {
        NodeSession p1 = proxy("proxy-01");
        // Hello after an Agora restart: the proxy lists a player the presence does not know.
        PresenceEntry known = PresenceEntry.newBuilder(entry(STEVE, "Steve")).setServer("lobby-01").build();
        presence.onHello(p1, Hello.newBuilder().setNodeId("proxy-01").setKind(NodeKind.VELOCITY).addPlayers(known).build());
        assertThat(presence.snapshot()).hasSize(1);
        // Heartbeat corrects the server.
        presence.onHeartbeat(p1, Heartbeat.newBuilder().addPlayers(PresenceEntry.newBuilder(known).setServer("survival-01")).build());
        assertThat(presence.snapshot()).extracting(PresenceEntry::getServer).containsExactly("survival-01");
        // Heartbeat without the player removes it once past the grace period (E11).
        offset.set(Duration.ofSeconds(11));
        presence.onHeartbeat(p1, Heartbeat.getDefaultInstance());
        assertThat(presence.snapshot()).isEmpty();

        assertThat(flushed()).isEmpty();
    }

    @Test
    void serverIsProxy() throws Exception {
        NodeSession p1 = proxy("proxy-07");
        claim(p1, STEVE);
        switched(p1, STEVE, "lobby-01");
        switched(p1, STEVE, "survival-01");
        left(p1, STEVE);

        assertThat(flushed()).hasSize(3).allSatisfy(e -> assertThat(e.getServer()).isEqualTo("proxy-07"));
    }

    @Test
    void presenceDoesNotWaitForDatabase() throws Exception {
        dbGate = new CountDownLatch(1); // Postgres hangs
        NodeSession p1 = proxy("proxy-01");
        claim(p1, STEVE);

        CompletableFuture<Void> done = CompletableFuture.runAsync(() -> {
            switched(p1, STEVE, "lobby-01");
            left(p1, STEVE);
        });
        done.get(2, TimeUnit.SECONDS); // presence moved on at once
        assertThat(presence.snapshot()).isEmpty();
        // let the scheduler run into the blocked insert, then keep adding: still no blocking
        Thread.sleep(1_300);
        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            sink.add(StoredAuditEntry.getDefaultInstance());
        }
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(500);

        dbGate.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> written.size() >= 102);
    }

    @Test
    void sinkWritesWithinASecondWithoutFlushCall() {
        sink.add(StoredAuditEntry.getDefaultInstance());
        await().atMost(Duration.ofMillis(1_500)).until(() -> written.size() == 1);
    }

    @Test
    void failedInsertIsRetried() {
        List<StoredAuditEntry> seen = new ArrayList<>();
        int[] calls = {0};
        AuditStore flaky = new AuditStore(null) {
            @Override
            public void insert(List<StoredAuditEntry> rows) throws SQLException {
                if (calls[0]++ == 0) throw new SQLException("down");
                seen.addAll(rows);
            }
        };
        AuditSink s = new AuditSink(flaky, scheduler);
        s.add(StoredAuditEntry.getDefaultInstance());
        s.flush(); // fails, entry stays buffered
        assertThat(seen).isEmpty();
        s.flush();
        assertThat(seen).hasSize(1);
    }
}

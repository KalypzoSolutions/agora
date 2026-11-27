package de.kalypzo.agora.presence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import de.kalypzo.agora.auth.AuthInterceptor;
import de.kalypzo.agora.auth.NodeTokens;
import de.kalypzo.agora.grpc.NodeStream;
import de.kalypzo.agora.grpc.PresenceEndpoints;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.ClaimPresenceRequest;
import de.kalypzo.agora.proto.v1.ClaimPresenceResponse;
import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.KickPlayer;
import de.kalypzo.agora.proto.v1.KickResult;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PlayerLeft;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.PresenceChanged;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PresenceServiceTest {

    static final Instant T0 = Instant.parse("2026-11-27T12:00:00Z");
    static final String STEVE = "00000000-0000-0000-0000-000000000001";
    static final String ALEX = "00000000-0000-0000-0000-000000000002";
    static final String BOB = "00000000-0000-0000-0000-000000000003";

    @TempDir Path dir;
    final MutableClock clock = new MutableClock(T0);
    final List<String> events = new CopyOnWriteArrayList<>();
    final List<Client> clients = new ArrayList<>();
    final Map<String, CompletableFuture<Void>> markers = new ConcurrentHashMap<>();
    ScheduledExecutorService scheduler;
    ExecutorService lanes;
    NodeRegistry registry;
    NodeMessageDispatcher dispatcher;
    PresenceService presence;
    Server server;
    ManagedChannel channel;

    final PresenceEvents recorder = new PresenceEvents() {
        @Override
        public void switched(PresenceEntry entry, String previousServer) {
            events.add("switched " + entry.getName() + " " + previousServer + "->" + entry.getServer());
        }

        @Override
        public void removed(PresenceEntry entry, RemovalReason reason) {
            events.add("removed " + entry.getName() + " " + entry.getProxy() + " " + reason);
        }
    };

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        lanes = Executors.newVirtualThreadPerTaskExecutor();
        registry = new NodeRegistry(lanes);
        dispatcher = new NodeMessageDispatcher();
    }

    void start(Duration kickTimeout) throws Exception {
        Path tokens = dir.resolve("tokens");
        Files.writeString(tokens, "paper-1=tok-paper\nproxy-01=tok-p1\nproxy-02=tok-p2\n");
        presence = new PresenceService(registry, clock, kickTimeout, recorder);
        presence.install(dispatcher);
        dispatcher.addListener(new NodeMessageListener() {
            @Override
            public void onKickResult(NodeSession session, KickResult result) {
                CompletableFuture<Void> f = markers.remove(result.getRequestId());
                if (f != null) f.complete(null);
            }
        });
        var service = new de.kalypzo.agora.service.AgoraService();
        PresenceEndpoints.register(service, presence, registry);
        service.onConnect(new NodeStream(registry, dispatcher, presence, clock, scheduler, Duration.ZERO,
                NodeSession.DEFAULT_QUEUE_LIMIT));
        String name = "presence-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name)
                .addService(service)
                .intercept(new AuthInterceptor(NodeTokens.load(tokens)))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    void start() throws Exception {
        start(PresenceService.DEFAULT_KICK_TIMEOUT);
    }

    @AfterEach
    void tearDown() {
        clients.forEach(Client::cancel);
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
        scheduler.shutdownNow();
        lanes.shutdownNow();
    }

    // ---------- tests ----------

    @Test
    void claimCreatesEntryAndBroadcasts() throws Exception {
        start();
        Client paper = connect("paper-1", "tok-paper", NodeKind.PAPER);
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);

        PresenceEntry claimed = PresenceEntry.newBuilder(entry(STEVE, "Steve"))
                .setProxy("forged").setServer("lobby-01").setOnlineSinceMillis(1L).build();
        assertThat(claim("proxy-01", "tok-p1", claimed).getAccepted()).isTrue();

        PresenceEntry expected = PresenceEntry.newBuilder(claimed)
                .setProxy("proxy-01").setServer("").setOnlineSinceMillis(T0.toEpochMilli()).build();
        assertThat(presence.snapshot()).containsExactly(expected);
        assertThat(paper.nextPresence().getUpsert()).isEqualTo(expected);
        assertThat(p1.nextPresence().getUpsert()).isEqualTo(expected);
        // A later Welcome carries the entry (PresenceSnapshot wiring).
        Client late = connect("proxy-02", "tok-p2", NodeKind.VELOCITY, false);
        assertThat(late.next().getWelcome().getPresenceList()).containsExactly(expected);
        assertThat(events).isEmpty(); // a claim alone is no audit event
    }

    @Test
    void secondProxyClaimKicksOldProxyFirst() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        Client p2 = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        CompletableFuture<ClaimPresenceResponse> second = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        KickPlayer kick = p1.nextMatching(ServiceMessage::hasKickPlayer).getKickPlayer();
        assertThat(kick.getPlayer()).isEqualTo(STEVE);
        assertThat(kick.getRequestId()).isNotBlank();
        // The answer to proxy-02 comes only after the old proxy answered.
        Thread.sleep(100);
        assertThat(second).isNotDone();
        p1.send(NodeMessage.newBuilder()
                .setKickResult(KickResult.newBuilder().setRequestId(kick.getRequestId()).setWasOnline(true)).build());

        assertThat(second.get(5, TimeUnit.SECONDS).getAccepted()).isTrue();
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
        assertThat(p2.nextMatching(m -> m.hasPresenceChanged()
                && m.getPresenceChanged().getUpsert().getProxy().equals("proxy-02"))).isNotNull();
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
    }

    @Test
    void ghostEntryIsReplaced() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        CompletableFuture<ClaimPresenceResponse> second = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        KickPlayer kick = p1.nextMatching(ServiceMessage::hasKickPlayer).getKickPlayer();
        p1.send(NodeMessage.newBuilder()
                .setKickResult(KickResult.newBuilder().setRequestId(kick.getRequestId()).setWasOnline(false)).build());

        assertThat(second.get(5, TimeUnit.SECONDS).getAccepted()).isTrue();
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
    }

    @Test
    void silentOldProxyTimesOut() throws Exception {
        start(Duration.ofMillis(200));
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        long started = System.nanoTime();
        assertThat(claim("proxy-02", "tok-p2", entry(STEVE, "Steve")).getAccepted()).isTrue();
        long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertThat(tookMs).isBetween(150L, 2_500L);
        assertThat(p1.nextMatching(ServiceMessage::hasKickPlayer)).isNotNull();
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
    }

    @Test
    void oldProxyWithoutSessionIsNotAwaited() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();
        // The entry lingers while proxy-01's stream is already gone: hold its lane so node-gone has not run yet.
        CountDownLatch hold = new CountDownLatch(1);
        registry.serialize("proxy-01", () -> {
            try {
                hold.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        p1.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("proxy-01").isEmpty());
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-01"));

        long started = System.nanoTime();
        assertThat(claim("proxy-02", "tok-p2", entry(STEVE, "Steve")).getAccepted()).isTrue();
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(2_000L);
        assertThat(p1.kicks).isEmpty();
        hold.countDown();
        lane("proxy-01", STEVE);
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
    }

    @Test
    void concurrentClaimsAreSerialized() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        Client p2 = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        p1.answerKicks = true;
        p2.answerKicks = true;

        CompletableFuture<ClaimPresenceResponse> a = claimAsync("proxy-01", "tok-p1", entry(STEVE, "Steve"));
        CompletableFuture<ClaimPresenceResponse> b = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        assertThat(a.get(5, TimeUnit.SECONDS).getAccepted()).isTrue();
        assertThat(b.get(5, TimeUnit.SECONDS).getAccepted()).isTrue();

        List<PresenceEntry> snapshot = presence.snapshot();
        assertThat(snapshot).hasSize(1);
        String owner = snapshot.getFirst().getProxy();
        Client loser = owner.equals("proxy-01") ? p2 : p1;
        Client winner = owner.equals("proxy-01") ? p1 : p2;
        assertThat(loser.kicks).hasSize(1);
        assertThat(winner.kicks).isEmpty();
    }

    @Test
    void switchFromNonOwnerIgnored() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        Client p2 = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        p2.send(switched(STEVE, "survival-01"));
        p2.send(left(STEVE));
        barrier(p2, STEVE);
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getServer()).isEmpty());
        assertThat(events).isEmpty();

        // The owner's switch counts.
        p1.send(switched(STEVE, "lobby-01"));
        barrier(p1, STEVE);
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getServer()).isEqualTo("lobby-01"));
        assertThat(p2.nextMatching(m -> m.hasPresenceChanged()
                && m.getPresenceChanged().getUpsert().getServer().equals("lobby-01"))).isNotNull();
        p1.send(switched(STEVE, "survival-01"));
        p1.send(left(STEVE));
        barrier(p1, STEVE);
        assertThat(presence.snapshot()).isEmpty();
        assertThat(p2.nextMatching(m -> m.hasPresenceChanged()
                && m.getPresenceChanged().getRemoved().equals(STEVE))).isNotNull();
        assertThat(events).containsExactly(
                "switched Steve ->lobby-01",
                "switched Steve lobby-01->survival-01",
                "removed Steve proxy-01 LEFT");
    }

    @Test
    void leftDuringDoubleLoginCountsAsReplaced() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        CompletableFuture<ClaimPresenceResponse> second = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        KickPlayer kick = p1.nextMatching(ServiceMessage::hasKickPlayer).getKickPlayer();
        // The old proxy kicks: its disconnect handler reports PlayerLeft before the KickResult (R23).
        p1.send(left(STEVE));
        p1.send(NodeMessage.newBuilder()
                .setKickResult(KickResult.newBuilder().setRequestId(kick.getRequestId()).setWasOnline(true)).build());
        assertThat(second.get(5, TimeUnit.SECONDS).getAccepted()).isTrue();
        barrier(p1, STEVE);

        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
    }

    @Test
    void proxyGoneRemovesItsPlayers() throws Exception {
        start();
        Client paper = connect("paper-1", "tok-paper", NodeKind.PAPER);
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        claim("proxy-01", "tok-p1", entry(STEVE, "Steve"));
        claim("proxy-01", "tok-p1", entry(ALEX, "Alex"));
        claim("proxy-02", "tok-p2", entry(BOB, "Bob"));

        p1.cancel();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(presence.snapshot())
                .extracting(PresenceEntry::getName).containsExactly("Bob"));
        List<String> removed = new ArrayList<>();
        removed.add(paper.nextMatching(m -> !m.getPresenceChanged().getRemoved().isEmpty())
                .getPresenceChanged().getRemoved());
        removed.add(paper.nextMatching(m -> !m.getPresenceChanged().getRemoved().isEmpty())
                .getPresenceChanged().getRemoved());
        assertThat(removed).containsExactlyInAnyOrder(STEVE, ALEX);
        assertThat(events).containsExactlyInAnyOrder(
                "removed Steve proxy-01 PROXY_LOST", "removed Alex proxy-01 PROXY_LOST");
    }

    @Test
    void reconnectedProxyKeepsItsPlayers() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        claim("proxy-01", "tok-p1", entry(STEVE, "Steve"));
        // R2: the new session supersedes the old one; ending the old one is not node-gone.
        Client again = client("proxy-01", "tok-p1");
        again.send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId("proxy-01")
                .setKind(NodeKind.VELOCITY).addPlayers(presence.snapshot().getFirst())).build());
        assertThat(again.nextMatching(ServiceMessage::hasWelcome)).isNotNull();
        p1.cancel();
        lane("proxy-01", STEVE);
        assertThat(presence.snapshot()).extracting(PresenceEntry::getName).containsExactly("Steve");
        assertThat(events).isEmpty();
    }

    @Test
    void shutdownRemovesNothing() throws Exception {
        start();
        connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        claim("proxy-01", "tok-p1", entry(STEVE, "Steve"));
        registry.shutdown(Status.UNAVAILABLE);
        lane("proxy-01", STEVE);
        assertThat(presence.snapshot()).extracting(PresenceEntry::getName).containsExactly("Steve");
        assertThat(events).isEmpty();
    }

    @Test
    void heartbeatRemovesStaleButKeepsFreshClaim() throws Exception {
        start();
        Client paper = connect("paper-1", "tok-paper", NodeKind.PAPER);
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();
        paper.nextPresence(); // the claim's upsert

        // Heartbeat built before Steve was in it, 1 s after the claim: the entry stays (E11).
        clock.set(T0.plusSeconds(1));
        p1.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        barrier(p1, STEVE);
        assertThat(presence.snapshot()).extracting(PresenceEntry::getName).containsExactly("Steve");
        assertThat(paper.poll(Duration.ofMillis(100))).isNull();

        // 11 s after the claim: stale, removed and broadcast, but no audit event.
        clock.set(T0.plusSeconds(11));
        p1.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        barrier(p1, STEVE);
        assertThat(presence.snapshot()).isEmpty();
        assertThat(paper.nextPresence().getRemoved()).isEqualTo(STEVE);
        assertThat(events).isEmpty();
    }

    @Test
    void helloAndHeartbeatAddMissingAndUpdateServer() throws Exception {
        start();
        Client paper = connect("paper-1", "tok-paper", NodeKind.PAPER);
        Client p2 = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        claim("proxy-02", "tok-p2", entry(BOB, "Bob"));
        paper.nextPresence();

        Client p1 = client("proxy-01", "tok-p1");
        PresenceEntry steve = PresenceEntry.newBuilder(entry(STEVE, "Steve")).setServer("lobby-01")
                .setProxy("proxy-01").setOnlineSinceMillis(5L).build();
        // Bob belongs to proxy-02: a foreign proxy's list does not take him over.
        PresenceEntry bobForeign = PresenceEntry.newBuilder(entry(BOB, "Bob")).setServer("x").setProxy("proxy-01").build();
        p1.send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId("proxy-01")
                .setKind(NodeKind.VELOCITY).addPlayers(steve).addPlayers(bobForeign)).build());
        assertThat(p1.nextMatching(ServiceMessage::hasWelcome).getWelcome().getPresenceList())
                .extracting(PresenceEntry::getName).containsExactlyInAnyOrder("Steve", "Bob");
        assertThat(paper.nextPresence().getUpsert()).isEqualTo(steve);

        p1.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder()
                .addPlayers(PresenceEntry.newBuilder(steve).setServer("survival-01"))).build());
        barrier(p1, STEVE);
        assertThat(paper.nextPresence().getUpsert().getServer()).isEqualTo("survival-01");
        assertThat(presence.snapshot()).filteredOn(e -> e.getName().equals("Bob"))
                .singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
        assertThat(events).isEmpty(); // reconciliation is no audit event
        p2.cancel();
    }

    @Test
    void claimerRestartDuringKickWaitIsNotAccepted() throws Exception {
        start(Duration.ofMillis(500));
        Client paper = connect("paper-1", "tok-paper", NodeKind.PAPER);
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        Client p2 = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        CompletableFuture<ClaimPresenceResponse> second = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        assertThat(p1.nextMatching(ServiceMessage::hasKickPlayer)).isNotNull(); // proxy-01 stays silent
        // proxy-02 restarts during the wait: a new session with the same node id supersedes the claiming one.
        Client restarted = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(p2.status()).isEqualTo(Status.Code.ABORTED);

        assertThat(second.get(5, TimeUnit.SECONDS).getAccepted()).isFalse();
        assertThat(presence.snapshot()).isEmpty(); // no ghost for proxy-02, the old entry is removed anyway
        assertThat(paper.nextMatching(m -> m.getPresenceChanged().getRemoved().equals(STEVE))).isNotNull();
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
        restarted.cancel();
    }

    @Test
    void claimerGoneDuringKickWaitIsNotAccepted() throws Exception {
        start(Duration.ofMillis(500));
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        Client p2 = connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        CompletableFuture<ClaimPresenceResponse> second = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        assertThat(p1.nextMatching(ServiceMessage::hasKickPlayer)).isNotNull();
        p2.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("proxy-02").isEmpty());

        assertThat(second.get(5, TimeUnit.SECONDS).getAccepted()).isFalse();
        assertThat(presence.snapshot()).isEmpty();
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
    }

    @Test
    void heartbeatOfKickedProxyDuringWaitIsSkipped() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        connect("proxy-02", "tok-p2", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();
        clock.set(T0.plusSeconds(20)); // the entry is long past the grace

        CompletableFuture<ClaimPresenceResponse> second = claimAsync("proxy-02", "tok-p2", entry(STEVE, "Steve"));
        KickPlayer kick = p1.nextMatching(ServiceMessage::hasKickPlayer).getKickPlayer();
        // proxy-01 already dropped Steve from its list; the claim in flight decides, not the heartbeat.
        p1.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        processed(p1);
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-01"));

        p1.send(NodeMessage.newBuilder()
                .setKickResult(KickResult.newBuilder().setRequestId(kick.getRequestId()).setWasOnline(true)).build());
        assertThat(second.get(5, TimeUnit.SECONDS).getAccepted()).isTrue();
        assertThat(presence.snapshot()).singleElement().satisfies(e -> assertThat(e.getProxy()).isEqualTo("proxy-02"));
        assertThat(events).containsExactly("removed Steve proxy-01 REPLACED");
    }

    @Test
    void graceEndsAtExactlyTenSeconds() throws Exception {
        start();
        Client p1 = connect("proxy-01", "tok-p1", NodeKind.VELOCITY);
        assertThat(claim("proxy-01", "tok-p1", entry(STEVE, "Steve")).getAccepted()).isTrue();

        clock.set(T0.plusSeconds(10).minusMillis(1));
        p1.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        barrier(p1, STEVE);
        assertThat(presence.snapshot()).hasSize(1);

        clock.set(T0.plusSeconds(10));
        p1.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        barrier(p1, STEVE);
        assertThat(presence.snapshot()).isEmpty();
    }

    @Test
    void paperCannotClaim() throws Exception {
        start();
        connect("paper-1", "tok-paper", NodeKind.PAPER);
        assertThatThrownBy(() -> claim("paper-1", "tok-paper", entry(STEVE, "Steve")))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
        // Without an open session: FAILED_PRECONDITION.
        assertThatThrownBy(() -> claim("proxy-01", "tok-p1", entry(STEVE, "Steve")))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertThat(presence.snapshot()).isEmpty();
    }

    // ---------- helpers ----------

    static PresenceEntry entry(String player, String name) {
        return PresenceEntry.newBuilder().setPlayer(player).setName(name).setLocale("de_DE").build();
    }

    static NodeMessage switched(String player, String server) {
        return NodeMessage.newBuilder()
                .setPlayerSwitched(PlayerSwitched.newBuilder().setPlayer(player).setServer(server)).build();
    }

    static NodeMessage left(String player) {
        return NodeMessage.newBuilder().setPlayerLeft(PlayerLeft.newBuilder().setPlayer(player)).build();
    }

    /** Waits for the node's lane (tasks queued so far) and then for the player's claim chain. */
    void lane(String nodeId, String player) throws Exception {
        registry.serialize(nodeId, () -> {}).get(5, TimeUnit.SECONDS);
        presence.barrier(UUID.fromString(player)).get(5, TimeUnit.SECONDS);
    }

    /**
     * Waits until the server has handled every message {@code c} sent so far (a marker message seen by a listener
     * registered after the presence service, on the same lane), then for the player's claim chain.
     */
    void barrier(Client c, String player) throws Exception {
        processed(c);
        presence.barrier(UUID.fromString(player)).get(5, TimeUnit.SECONDS);
    }

    /** Waits until the server has handled every message {@code c} sent so far (without the claim chain). */
    void processed(Client c) throws Exception {
        String id = "marker-" + System.nanoTime();
        CompletableFuture<Void> seen = new CompletableFuture<>();
        markers.put(id, seen);
        c.send(NodeMessage.newBuilder().setKickResult(KickResult.newBuilder().setRequestId(id)).build());
        seen.get(5, TimeUnit.SECONDS);
    }

    Metadata headers(String node, String token) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), node);
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return md;
    }

    ClaimPresenceResponse claim(String node, String token, PresenceEntry e) {
        return AgoraGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node, token)))
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .claimPresence(ClaimPresenceRequest.newBuilder().setEntry(e).build());
    }

    CompletableFuture<ClaimPresenceResponse> claimAsync(String node, String token, PresenceEntry e) {
        CompletableFuture<ClaimPresenceResponse> f = new CompletableFuture<>();
        AgoraGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node, token)))
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .claimPresence(ClaimPresenceRequest.newBuilder().setEntry(e).build(), new StreamObserver<>() {
                    @Override public void onNext(ClaimPresenceResponse r) { f.complete(r); }
                    @Override public void onError(Throwable t) { f.completeExceptionally(t); }
                    @Override public void onCompleted() {}
                });
        return f;
    }

    Client client(String node, String token) {
        Client c = new Client();
        AgoraGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node, token)))
                .connect(c);
        clients.add(c);
        return c;
    }

    Client connect(String node, String token, NodeKind kind) throws Exception {
        return connect(node, token, kind, true);
    }

    Client connect(String node, String token, NodeKind kind, boolean consumeWelcome) throws Exception {
        Client c = client(node, token);
        c.send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId(node).setKind(kind)).build());
        if (consumeWelcome) assertThat(c.next().hasWelcome()).isTrue();
        return c;
    }

    static final class Client implements ClientResponseObserver<NodeMessage, ServiceMessage> {
        final LinkedBlockingQueue<ServiceMessage> received = new LinkedBlockingQueue<>();
        final List<KickPlayer> kicks = new CopyOnWriteArrayList<>();
        final CompletableFuture<Status> closed = new CompletableFuture<>();
        volatile ClientCallStreamObserver<NodeMessage> requests;
        volatile boolean answerKicks;

        @Override public void beforeStart(ClientCallStreamObserver<NodeMessage> r) { this.requests = r; }

        @Override
        public void onNext(ServiceMessage value) {
            if (value.hasKickPlayer()) {
                kicks.add(value.getKickPlayer());
                if (answerKicks) {
                    send(NodeMessage.newBuilder().setKickResult(KickResult.newBuilder()
                            .setRequestId(value.getKickPlayer().getRequestId()).setWasOnline(true)).build());
                }
            }
            received.add(value);
        }

        @Override public void onError(Throwable t) { closed.complete(Status.fromThrowable(t)); }
        @Override public void onCompleted() { closed.complete(Status.OK); }

        synchronized void send(NodeMessage m) {
            requests.onNext(m);
        }

        void cancel() {
            if (requests != null && !closed.isDone()) requests.cancel("test done", null);
        }

        ServiceMessage next() throws InterruptedException {
            ServiceMessage m = received.poll(5, TimeUnit.SECONDS);
            assertThat(m).as("expected a message within 5 s").isNotNull();
            return m;
        }

        PresenceChanged nextPresence() throws InterruptedException {
            return nextMatching(ServiceMessage::hasPresenceChanged).getPresenceChanged();
        }

        /** Skips messages until one matches (PresenceChanged-predicates see only PresenceChanged messages). */
        ServiceMessage nextMatching(Predicate<ServiceMessage> p) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < until) {
                ServiceMessage m = received.poll(until - System.nanoTime(), TimeUnit.NANOSECONDS);
                if (m == null) break;
                if (p.test(m)) return m;
            }
            throw new AssertionError("no matching message within 5 s");
        }

        Status.Code status() throws Exception {
            return closed.get(5, TimeUnit.SECONDS).getCode();
        }

        ServiceMessage poll(Duration d) throws InterruptedException {
            return received.poll(d.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void set(Instant i) {
            now = i;
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}

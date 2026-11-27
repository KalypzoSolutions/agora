package de.kalypzo.agora.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import de.kalypzo.agora.auth.AuthInterceptor;
import de.kalypzo.agora.auth.NodeTokens;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.DocChanged;
import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.KickResult;
import de.kalypzo.agora.proto.v1.LocationReply;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PlayerLeft;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ReleasePlayer;
import de.kalypzo.agora.proto.v1.SendDelivery;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.UnwatchStore;
import de.kalypzo.agora.proto.v1.WatchStore;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeStreamTest {

    static final Instant T0 = Instant.parse("2026-11-27T12:00:00Z");
    static final PresenceEntry ALICE = PresenceEntry.newBuilder()
            .setPlayer("00000000-0000-0000-0000-000000000001")
            .setName("Alice")
            .setServer("lobby-01")
            .setProxy("velocity-1")
            .setLocale("de_DE")
            .setOnlineSinceMillis(1_000L)
            .build();

    @TempDir Path dir;
    final MutableClock clock = new MutableClock(T0);
    final List<PresenceEntry> presence = new CopyOnWriteArrayList<>();
    final List<NodeSession> gone = new CopyOnWriteArrayList<>();
    final List<Client> clients = new ArrayList<>();
    ScheduledExecutorService scheduler;
    ExecutorService lanes;
    NodeRegistry registry;
    NodeMessageDispatcher dispatcher;
    NodeStream stream;
    Server server;
    ManagedChannel channel;
    String name;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        lanes = Executors.newVirtualThreadPerTaskExecutor();
        registry = new NodeRegistry(lanes);
        registry.onNodeGone(gone::add);
        dispatcher = new NodeMessageDispatcher();
    }

    void start(Duration paperDelay, int queueLimit) throws Exception {
        Path tokens = dir.resolve("tokens");
        Files.writeString(tokens, "paper-1=tok-paper\nvelocity-1=tok-velocity\n");
        stream = new NodeStream(registry, dispatcher, () -> List.copyOf(presence), clock, scheduler, paperDelay,
                queueLimit);
        name = "node-stream-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name)
                .addService(new AgoraService().onConnect(stream))
                .intercept(new AuthInterceptor(NodeTokens.load(tokens)))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    void start() throws Exception {
        start(Duration.ZERO, NodeSession.DEFAULT_QUEUE_LIMIT);
    }

    @AfterEach
    void tearDown() {
        clients.forEach(Client::cancel);
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
        scheduler.shutdownNow();
        lanes.shutdownNow();
    }

    /** Waits until every lifecycle/message task queued so far for {@code nodeId} has run. */
    void flush(String nodeId) throws Exception {
        registry.serialize(nodeId, () -> {}).get(5, TimeUnit.SECONDS);
    }

    // ---------- tests ----------

    @Test
    void firstMessageMustBeHello() throws Exception {
        start();
        Client c = client("paper-1", "tok-paper", -1);
        c.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        assertThat(c.status()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(registry.get("paper-1")).isEmpty();
    }

    @Test
    void helloNodeIdMustMatchToken() throws Exception {
        start();
        Client c = client("paper-1", "tok-paper", -1);
        c.hello("velocity-1", NodeKind.PAPER);
        assertThat(c.status()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(registry.all()).isEmpty();
    }

    @Test
    void welcomeContainsPresenceSnapshot() throws Exception {
        presence.add(ALICE);
        start();
        Client c = client("velocity-1", "tok-velocity", -1);
        c.hello("velocity-1", NodeKind.VELOCITY);
        ServiceMessage m = c.next();
        assertThat(m.hasWelcome()).isTrue();
        assertThat(m.getWelcome().getPresenceList()).containsExactly(ALICE);
        assertThat(registry.get("velocity-1")).hasValueSatisfying(s -> {
            assertThat(s.nodeId()).isEqualTo("velocity-1");
            assertThat(s.kind()).isEqualTo(NodeKind.VELOCITY);
        });
    }

    @Test
    void secondSessionWithSameIdReplacesFirst() throws Exception {
        start();
        Client first = client("paper-1", "tok-paper", -1);
        first.hello("paper-1", NodeKind.PAPER);
        assertThat(first.next().hasWelcome()).isTrue();
        NodeSession firstSession = registry.get("paper-1").orElseThrow();

        Client second = client("paper-1", "tok-paper", -1);
        second.hello("paper-1", NodeKind.PAPER);
        assertThat(second.next().hasWelcome()).isTrue();

        assertThat(first.status()).isEqualTo(Status.Code.ABORTED);
        NodeSession secondSession = registry.get("paper-1").orElseThrow();
        assertThat(secondSession).isNotSameAs(firstSession);
        assertThat(registry.all()).containsExactly(secondSession);
        // R2: ending the replaced session is not node-gone for the new one.
        flush("paper-1");
        assertThat(gone).isEmpty();
        assertThat(registry.isSuperseded(firstSession)).isTrue();
        assertThat(registry.isSuperseded(secondSession)).isFalse();
    }

    @Test
    void replacedSessionEndingLaterDoesNotFireNodeGone() throws Exception {
        start();
        Client first = client("paper-1", "tok-paper", -1);
        first.hello("paper-1", NodeKind.PAPER);
        assertThat(first.next().hasWelcome()).isTrue();
        NodeSession firstSession = registry.get("paper-1").orElseThrow();
        Client second = client("paper-1", "tok-paper", -1);
        second.hello("paper-1", NodeKind.PAPER);
        assertThat(second.next().hasWelcome()).isTrue();
        assertThat(first.status()).isEqualTo(Status.Code.ABORTED);

        // The old session reports its end once more (e.g. late peer close): still no node-gone, new one stays.
        firstSession.close(Status.CANCELLED);
        flush("paper-1");
        assertThat(gone).isEmpty();
        assertThat(registry.get("paper-1")).isPresent();

        second.complete();
        await().atMost(Duration.ofSeconds(5)).until(() -> gone.size() == 1);
        assertThat(gone.getFirst().nodeId()).isEqualTo("paper-1");
        assertThat(registry.get("paper-1")).isEmpty();
    }

    @Test
    void slowReaderIsDisconnectedAtLimit() throws Exception {
        start(Duration.ZERO, 100);
        Client c = client("paper-1", "tok-paper", 1); // reads exactly one message (the Welcome), then stops reading
        c.hello("paper-1", NodeKind.PAPER);
        assertThat(c.next().hasWelcome()).isTrue();
        NodeSession s = registry.get("paper-1").orElseThrow();
        ServiceMessage change = ServiceMessage.newBuilder().setDocChanged(DocChanged.newBuilder().setVersion(1)).build();
        for (int i = 0; i < 100; i++) {
            assertThat(s.send(change)).as("message %d", i + 1).isTrue();
        }
        assertThat(s.send(change)).as("message 101").isFalse();
        assertThat(c.status()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        assertThat(s.send(change)).isFalse();
    }

    @Test
    void paperWelcomeWaitsForWarmup() throws Exception {
        start(Duration.ofSeconds(15), NodeSession.DEFAULT_QUEUE_LIMIT);
        presence.add(ALICE);
        clock.set(T0.plusSeconds(5));
        Client c = client("paper-1", "tok-paper", -1);
        c.hello("paper-1", NodeKind.PAPER);
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("paper-1").isPresent());
        flush("paper-1");
        stream.tick();
        // Other messages flow normally before the Welcome.
        registry.get("paper-1").orElseThrow().send(
                ServiceMessage.newBuilder().setDocChanged(DocChanged.newBuilder().setVersion(7)).build());
        assertThat(c.next().hasDocChanged()).isTrue();
        assertThat(c.poll(Duration.ofMillis(200))).isNull();

        clock.set(T0.plusSeconds(15));
        stream.tick();
        ServiceMessage m = c.next();
        assertThat(m.hasWelcome()).isTrue();
        assertThat(m.getWelcome().getPresenceList()).containsExactly(ALICE);
        stream.tick();
        assertThat(c.poll(Duration.ofMillis(200))).isNull(); // exactly one Welcome
    }

    @Test
    void velocityWelcomeIsImmediate() throws Exception {
        start(Duration.ofSeconds(15), NodeSession.DEFAULT_QUEUE_LIMIT);
        clock.set(T0.plusSeconds(1));
        Client c = client("velocity-1", "tok-velocity", -1);
        c.hello("velocity-1", NodeKind.VELOCITY);
        assertThat(c.next().hasWelcome()).isTrue();
    }

    @Test
    void nodeGoneFiresOnStreamEnd() throws Exception {
        start();
        Client c = client("velocity-1", "tok-velocity", -1);
        c.hello("velocity-1", NodeKind.VELOCITY);
        assertThat(c.next().hasWelcome()).isTrue();
        c.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> gone.size() == 1);
        assertThat(gone.getFirst().nodeId()).isEqualTo("velocity-1");
        assertThat(gone.getFirst().kind()).isEqualTo(NodeKind.VELOCITY);
        assertThat(registry.get("velocity-1")).isEmpty();
    }

    @Test
    void shutdownClosesStreamsWithoutNodeGone() throws Exception {
        start();
        Client c = client("velocity-1", "tok-velocity", -1);
        c.hello("velocity-1", NodeKind.VELOCITY);
        assertThat(c.next().hasWelcome()).isTrue();
        registry.shutdown(Status.UNAVAILABLE.withDescription("shutting down"));
        assertThat(c.status()).isEqualTo(Status.Code.UNAVAILABLE);
        flush("velocity-1");
        assertThat(gone).isEmpty();
        Client late = client("paper-1", "tok-paper", -1);
        late.hello("paper-1", NodeKind.PAPER);
        assertThat(late.status()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void helloAndEveryPayloadAreDispatched() throws Exception {
        start();
        List<String> seen = new CopyOnWriteArrayList<>();
        List<Hello> hellos = new CopyOnWriteArrayList<>();
        dispatcher.addHelloListener((s, h) -> hellos.add(h));
        dispatcher.addListener(new NodeMessageListener() {
            @Override public void onHello(NodeSession s, Hello m) { seen.add("hello"); }
            @Override public void onHeartbeat(NodeSession s, Heartbeat m) { seen.add("heartbeat"); }
            @Override public void onWatchStore(NodeSession s, WatchStore m) { seen.add("watch"); }
            @Override public void onUnwatchStore(NodeSession s, UnwatchStore m) { seen.add("unwatch"); }
            @Override public void onKickResult(NodeSession s, KickResult m) { seen.add("kick"); }
            @Override public void onPlayerSwitched(NodeSession s, PlayerSwitched m) { seen.add("switched"); }
            @Override public void onPlayerLeft(NodeSession s, PlayerLeft m) { seen.add("left"); }
            @Override public void onLocationReply(NodeSession s, LocationReply m) { seen.add("location"); }
            @Override public void onSendDelivery(NodeSession s, SendDelivery m) { seen.add("delivery:" + s.nodeId()); }
            @Override public void onReleasePlayer(NodeSession s, ReleasePlayer m) { seen.add("release"); }
        });
        Client c = client("velocity-1", "tok-velocity", -1);
        c.send(NodeMessage.newBuilder().setHello(Hello.newBuilder()
                .setNodeId("velocity-1").setKind(NodeKind.VELOCITY)
                .addHeldPlayers(ALICE.getPlayer()).addWatchedStores("eco:shops").addPlayers(ALICE)).build());
        assertThat(c.next().hasWelcome()).isTrue();
        c.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        c.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore("eco:shops")).build());
        c.send(NodeMessage.newBuilder().setUnwatchStore(UnwatchStore.newBuilder().setStore("eco:shops")).build());
        c.send(NodeMessage.newBuilder().setKickResult(KickResult.newBuilder().setRequestId("r")).build());
        c.send(NodeMessage.newBuilder().setPlayerSwitched(PlayerSwitched.newBuilder().setPlayer("p")).build());
        c.send(NodeMessage.newBuilder().setPlayerLeft(PlayerLeft.newBuilder().setPlayer("p")).build());
        c.send(NodeMessage.newBuilder().setLocationReply(LocationReply.newBuilder().setRequestId("r")).build());
        c.send(NodeMessage.newBuilder().setSendDelivery(SendDelivery.newBuilder().setPlayer("p")).build());
        c.send(NodeMessage.newBuilder().setReleasePlayer(ReleasePlayer.newBuilder().setPlayer("p")).build());
        await().atMost(Duration.ofSeconds(5)).until(() -> seen.size() == 10);
        assertThat(seen).containsExactly("hello", "heartbeat", "watch", "unwatch", "kick", "switched", "left",
                "location", "delivery:velocity-1", "release");
        assertThat(hellos).hasSize(1);
        assertThat(hellos.getFirst().getHeldPlayersList()).containsExactly(ALICE.getPlayer());
        assertThat(hellos.getFirst().getWatchedStoresList()).containsExactly("eco:shops");
        assertThat(hellos.getFirst().getPlayersList()).containsExactly(ALICE);
    }

    /** A downstream component in the style of A-10: holders per node, set by Hello, removed by node-gone. */
    Map<String, List<String>> holderStateLikeA10(List<Boolean> supersededSeen) {
        Map<String, List<String>> holders = new ConcurrentHashMap<>();
        dispatcher.addHelloListener((s, h) -> holders.put(s.nodeId(), List.copyOf(h.getHeldPlayersList())));
        registry.onNodeGone(s -> {
            supersededSeen.add(registry.isSuperseded(s));
            if (!registry.isSuperseded(s)) holders.remove(s.nodeId());
        });
        return holders;
    }

    Client paperHello(String player) {
        Client c = client("paper-1", "tok-paper", -1);
        c.send(NodeMessage.newBuilder().setHello(Hello.newBuilder()
                .setNodeId("paper-1").setKind(NodeKind.PAPER).addHeldPlayers(player)).build());
        return c;
    }

    CountDownLatch blockLane(String nodeId) {
        CountDownLatch release = new CountDownLatch(1);
        registry.serialize(nodeId, () -> {
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return release;
    }

    @Test
    void nodeGoneOfOldSessionRacingNewHelloLeavesNewStateUntouched() throws Exception {
        start();
        List<Boolean> supersededSeen = new CopyOnWriteArrayList<>();
        Map<String, List<String>> holders = holderStateLikeA10(supersededSeen);
        Client old = paperHello("old-player");
        assertThat(old.next().hasWelcome()).isTrue();
        NodeSession oldSession = registry.get("paper-1").orElseThrow();

        // Lane held: the new Hello is queued first, then the old stream ends (node-gone queued behind the Hello).
        CountDownLatch release = blockLane("paper-1");
        Client fresh = paperHello("new-player");
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.pending("paper-1") == 2);
        old.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.get("paper-1").isEmpty());
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.pending("paper-1") == 3);

        release.countDown();
        assertThat(fresh.next().hasWelcome()).isTrue();
        flush("paper-1");
        assertThat(registry.isSuperseded(oldSession)).isTrue();
        // The superseded node-gone was dropped (R2); a listener that defers work can check isSuperseded itself.
        assertThat(gone).isEmpty();
        assertThat(supersededSeen).isEmpty();
        assertThat(holders).containsExactly(Map.entry("paper-1", List.of("new-player")));
    }

    @Test
    void nodeGoneOfOldSessionBeforeNewHelloIsAppliedFirst() throws Exception {
        start();
        List<Boolean> supersededSeen = new CopyOnWriteArrayList<>();
        Map<String, List<String>> holders = holderStateLikeA10(supersededSeen);
        Client old = paperHello("old-player");
        assertThat(old.next().hasWelcome()).isTrue();

        // Lane held: the old stream ends first (node-gone queued), then the node reconnects.
        CountDownLatch release = blockLane("paper-1");
        old.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.pending("paper-1") == 2);
        Client fresh = paperHello("new-player");
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.pending("paper-1") == 3);

        release.countDown();
        assertThat(fresh.next().hasWelcome()).isTrue();
        flush("paper-1");
        // Lane order: node-gone(old) cleans up, then the new Hello restores; the new state is untouched.
        assertThat(supersededSeen).containsExactly(false);
        assertThat(gone).hasSize(1);
        assertThat(holders).containsExactly(Map.entry("paper-1", List.of("new-player")));
    }

    @Test
    void stalledListenerStopsFurtherInboundDelivery() throws Exception {
        start();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        List<Heartbeat> beats = new CopyOnWriteArrayList<>();
        dispatcher.addListener(new NodeMessageListener() {
            @Override
            public void onHeartbeat(NodeSession s, Heartbeat m) {
                beats.add(m);
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        Client c = client("velocity-1", "tok-velocity", -1);
        c.hello("velocity-1", NodeKind.VELOCITY);
        assertThat(c.next().hasWelcome()).isTrue();
        for (int i = 0; i < 50; i++) {
            c.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        }
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200); // give an (incorrectly) auto-requesting server time to queue more
        assertThat(registry.pending("velocity-1")).isEqualTo(1); // only the stalled task, nothing queued behind it
        assertThat(beats).hasSize(1);

        release.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> beats.size() == 50);
    }

    @Test
    void messagesQueuedBeforeShutdownAreSkippedAndCloseWaitsForLanes() throws Exception {
        start();
        List<Heartbeat> beats = new CopyOnWriteArrayList<>();
        dispatcher.addListener(new NodeMessageListener() {
            @Override
            public void onHeartbeat(NodeSession s, Heartbeat m) {
                beats.add(m);
            }
        });
        Client c = client("velocity-1", "tok-velocity", -1);
        c.hello("velocity-1", NodeKind.VELOCITY);
        assertThat(c.next().hasWelcome()).isTrue();
        flush("velocity-1");
        CountDownLatch release = blockLane("velocity-1");
        c.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.pending("velocity-1") == 2);

        registry.shutdown(Status.UNAVAILABLE.withDescription("shutting down"));
        release.countDown();
        assertThat(registry.close(Duration.ofSeconds(5))).isTrue();
        assertThat(registry.pending("velocity-1")).isZero();
        assertThat(beats).isEmpty();
        assertThat(gone).isEmpty();
        assertThat(c.status()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void overflowRunsNodeGoneOffTheSenderAndOutsideTheSessionLock() throws Exception {
        start(Duration.ZERO, 3);
        Client velocity = client("velocity-1", "tok-velocity", -1);
        velocity.hello("velocity-1", NodeKind.VELOCITY);
        assertThat(velocity.next().hasWelcome()).isTrue();
        Client paper = client("paper-1", "tok-paper", 1);
        paper.hello("paper-1", NodeKind.PAPER);
        assertThat(paper.next().hasWelcome()).isTrue();
        NodeSession slow = registry.get("paper-1").orElseThrow();
        NodeSession proxy = registry.get("velocity-1").orElseThrow();

        CompletableFuture<Thread> listenerThread = new CompletableFuture<>();
        CompletableFuture<Boolean> heldLock = new CompletableFuture<>();
        registry.onNodeGone(s -> {
            heldLock.complete(Thread.holdsLock(s));
            // Broadcast to another session from inside node-gone (as A-11 will do).
            proxy.send(ServiceMessage.newBuilder().setDocChanged(DocChanged.newBuilder().setVersion(42)).build());
            listenerThread.complete(Thread.currentThread());
        });
        ServiceMessage change = ServiceMessage.newBuilder().setDocChanged(DocChanged.newBuilder().setVersion(1)).build();
        for (int i = 0; i < 3; i++) assertThat(slow.send(change)).isTrue();
        assertThat(slow.send(change)).isFalse();
        assertThat(paper.status()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);

        assertThat(listenerThread.get(5, TimeUnit.SECONDS)).isNotSameAs(Thread.currentThread());
        assertThat(heldLock.get()).isFalse();
        assertThat(velocity.next().getDocChanged().getVersion()).isEqualTo(42);
    }

    // ---------- helpers ----------

    /** @param initialRequests messages the client reads before it stops reading; -1 = automatic flow control */
    Client client(String node, String token, int initialRequests) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), node);
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        Client c = new Client(initialRequests);
        AgoraGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md)).connect(c);
        clients.add(c);
        return c;
    }

    static final class Client implements ClientResponseObserver<NodeMessage, ServiceMessage> {
        final int initialRequests;
        final LinkedBlockingQueue<ServiceMessage> received = new LinkedBlockingQueue<>();
        final CompletableFuture<Status> closed = new CompletableFuture<>();
        volatile ClientCallStreamObserver<NodeMessage> requests;

        Client(int initialRequests) {
            this.initialRequests = initialRequests;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<NodeMessage> requestStream) {
            this.requests = requestStream;
            if (initialRequests >= 0) requestStream.disableAutoRequestWithInitial(initialRequests);
        }

        @Override public void onNext(ServiceMessage value) { received.add(value); }
        @Override public void onError(Throwable t) { closed.complete(Status.fromThrowable(t)); }
        @Override public void onCompleted() { closed.complete(Status.OK); }

        synchronized void send(NodeMessage m) {
            requests.onNext(m);
        }

        void hello(String nodeId, NodeKind kind) {
            send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId(nodeId).setKind(kind)).build());
        }

        synchronized void complete() {
            requests.onCompleted();
        }

        void cancel() {
            if (requests != null && !closed.isDone()) requests.cancel("test done", null);
        }

        ServiceMessage next() throws InterruptedException {
            ServiceMessage m = received.poll(5, TimeUnit.SECONDS);
            assertThat(m).as("expected a message within 5 s").isNotNull();
            return m;
        }

        ServiceMessage poll(Duration d) throws InterruptedException {
            return received.poll(d.toMillis(), TimeUnit.MILLISECONDS);
        }

        Status.Code status() throws Exception {
            return closed.get(5, TimeUnit.SECONDS).getCode();
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

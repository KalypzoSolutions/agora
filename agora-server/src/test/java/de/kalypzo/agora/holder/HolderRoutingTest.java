package de.kalypzo.agora.holder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import de.kalypzo.agora.AgoraServer;
import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.grpc.PresenceEndpoints;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.ApplyRequest;
import de.kalypzo.agora.proto.v1.ApplyResponse;
import de.kalypzo.agora.proto.v1.DocChanged;
import de.kalypzo.agora.proto.v1.DocRef;
import de.kalypzo.agora.proto.v1.FieldOp;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.KickResult;
import de.kalypzo.agora.proto.v1.LoadPlayerRequest;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PlayerRow;
import de.kalypzo.agora.proto.v1.ReleasePlayer;
import de.kalypzo.agora.proto.v1.RowRef;
import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.StoreDoc;
import de.kalypzo.agora.proto.v1.UnwatchStore;
import de.kalypzo.agora.proto.v1.Value;
import de.kalypzo.agora.proto.v1.WatchStore;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-10: holder registry and DocChanged routing, end to end over gRPC and Postgres. */
class HolderRoutingTest {

    static final Instant T0 = Instant.parse("2026-11-27T12:00:00Z");
    static final String CLANS = "lobby:clans";
    static final FieldSpec COINS = FieldSpec.newBuilder().setNamespace("economy").setName("coins")
            .setType(ScalarType.LONG).setDefaultValue(Value.newBuilder().setLongValue(0)).build();
    static final FieldSpec SCORE = FieldSpec.newBuilder().setNamespace("lobby").setName("score")
            .setType(ScalarType.LONG).setDefaultValue(Value.newBuilder().setLongValue(0)).build();

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @TempDir Path dir;
    final MutableClock clock = new MutableClock(T0);
    final List<Client> clients = new ArrayList<>();
    final Map<String, CompletableFuture<Void>> markers = new ConcurrentHashMap<>();
    AgoraServer server;
    HolderRegistry holders;
    ManagedChannel channel;

    @BeforeAll
    static void startPostgres() {
        postgres.start();
    }

    @AfterAll
    static void stopPostgres() {
        postgres.stop();
    }

    AgoraConfig config() throws Exception {
        Path tokens = dir.resolve("tokens");
        Files.writeString(tokens, "lobby-01=tok-a\nlobby-02=tok-b\n");
        return new AgoraConfig(0, postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), 4, tokens,
                Duration.ZERO);
    }

    /** Explicit wiring with a controllable clock for the holder registry. */
    void start() throws Exception {
        AgoraService service = new AgoraService();
        NodeRegistry nodes = new NodeRegistry();
        NodeMessageDispatcher dispatcher = new NodeMessageDispatcher();
        PresenceService presence = new PresenceService(nodes);
        presence.install(dispatcher);
        PresenceEndpoints.register(service, presence, nodes);
        holders = new HolderRegistry(nodes, clock);
        holders.install(dispatcher);
        server = AgoraServer.start(config(), service, holders, nodes, dispatcher, presence, holders);
        afterStart();
    }

    /** The default start path must route real DocChanged events. */
    void startDefault() throws Exception {
        server = AgoraServer.start(config());
        afterStart();
    }

    void afterStart() {
        // Registered after the holder registry: sees a marker only after every earlier message of the node.
        server.nodeMessages().addListener(new NodeMessageListener() {
            @Override
            public void onKickResult(NodeSession session, KickResult result) {
                CompletableFuture<Void> f = markers.remove(result.getRequestId());
                if (f != null) f.complete(null);
            }
        });
        channel = NettyChannelBuilder.forAddress("localhost", server.port()).usePlaintext().build();
    }

    @AfterEach
    void tearDown() {
        clients.forEach(Client::cancel);
        if (channel != null) channel.shutdownNow();
        if (server != null) server.close();
    }

    // ---------- tests ----------

    @Test
    void holderReceivesDocChanged() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);

        String key = UUID.randomUUID().toString();
        ApplyResponse r = apply("lobby-02", "tok-b", key, player(p), inc(COINS, 5));

        DocChanged e = a.nextDocChanged();
        assertThat(e.getRef()).isEqualTo(playerRow(p, "economy"));
        assertThat(e.getVersion()).isEqualTo(r.getRows(0).getVersion());
        assertThat(e.getDeleted()).isFalse();
        assertThat(e.getDataMap()).containsEntry("coins", Value.newBuilder().setLongValue(5).build());
        assertThat(e.getIdempotencyKey()).isEqualTo(key);
    }

    @Test
    void nonHolderReceivesNothing() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        Client b = connect("lobby-02", "tok-b");
        UUID p = UUID.randomUUID();
        UUID marker = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        loadPlayer("lobby-02", "tok-b", marker);

        apply("lobby-01", "tok-a", key(), player(p), inc(COINS, 1));
        apply("lobby-01", "tok-a", key(), player(marker), inc(COINS, 1));
        // B gets the marker event (sent after p's change on the same stream), never p's.
        List<DocChanged> seen = b.docChangedUntil(e -> e.getRef().equals(playerRow(marker, "economy")));
        assertThat(seen).noneMatch(e -> e.getRef().getPlayerRow().getPlayer().equals(p.toString()));
        assertThat(a.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
    }

    @Test
    void releaseStopsDelivery() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        UUID marker = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        loadPlayer("lobby-01", "tok-a", marker);

        a.send(NodeMessage.newBuilder().setReleasePlayer(ReleasePlayer.newBuilder().setPlayer(p.toString())).build());
        processed(a);
        assertThat(holders.holdersOf(p)).isEmpty();

        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 1));
        apply("lobby-02", "tok-b", key(), player(marker), inc(COINS, 1));
        List<DocChanged> seen = a.docChangedUntil(e -> e.getRef().equals(playerRow(marker, "economy")));
        assertThat(seen).noneMatch(e -> e.getRef().getPlayerRow().getPlayer().equals(p.toString()));
    }

    @Test
    void watcherReceivesStoreChanges() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);

        String key = key();
        apply("lobby-02", "tok-b", key, store(CLANS, "red"), inc(SCORE, 7));
        DocChanged e = a.nextDocChanged();
        assertThat(e.getRef()).isEqualTo(storeRow(CLANS, "red"));
        assertThat(e.getDataMap()).containsEntry("score", Value.newBuilder().setLongValue(7).build());
        assertThat(e.getIdempotencyKey()).isEqualTo(key);

        // Unwatch stops delivery; a second watched store is the marker.
        String other = "lobby:other_" + Long.toHexString(System.nanoTime());
        a.send(NodeMessage.newBuilder().setUnwatchStore(UnwatchStore.newBuilder().setStore(CLANS)).build());
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(other)).build());
        processed(a);
        apply("lobby-02", "tok-b", key(), store(CLANS, "red"), inc(SCORE, 1));
        apply("lobby-02", "tok-b", key(), store(other, "marker"), inc(SCORE, 1));
        List<DocChanged> seen = a.docChangedUntil(x -> x.getRef().equals(storeRow(other, "marker")));
        assertThat(seen).noneMatch(x -> x.getRef().equals(storeRow(CLANS, "red")));
    }

    @Test
    void heartbeatRestoresLostHolder() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        // Agora does not know A as holder of p (lost); the heartbeat lists it.
        assertThat(holders.holdersOf(p)).isEmpty();
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().addHeldPlayers(p.toString())
                .addWatchedStores(CLANS)).build());
        processed(a);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");

        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 2));
        assertThat(a.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
        apply("lobby-02", "tok-b", key(), store(CLANS, "blue"), inc(SCORE, 2));
        assertThat(a.nextDocChanged().getRef()).isEqualTo(storeRow(CLANS, "blue"));
    }

    @Test
    void heartbeatReplacesSetsButKeepsFreshEntries() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID old = UUID.randomUUID();
        UUID fresh = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", old);
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);

        // 11 s later a new LoadPlayer, then a heartbeat built before it: fresh stays, old (unlisted) goes (R3).
        clock.set(T0.plusSeconds(11));
        loadPlayer("lobby-01", "tok-a", fresh);
        clock.set(T0.plusSeconds(12));
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        processed(a);
        assertThat(holders.holdersOf(old)).isEmpty();
        assertThat(holders.watchersOf(CLANS)).isEmpty();
        assertThat(holders.holdersOf(fresh)).containsExactly("lobby-01");

        // Exactly 10 s after the LoadPlayer the grace has ended.
        clock.set(T0.plusSeconds(21));
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
        processed(a);
        assertThat(holders.holdersOf(fresh)).isEmpty();
    }

    @Test
    void helloAfterReconnectRestoresHolders() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        a.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> holders.holdersOf(p).isEmpty());

        Client again = client("lobby-01", "tok-a");
        again.send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId("lobby-01").setKind(NodeKind.PAPER)
                .addHeldPlayers(p.toString()).addWatchedStores(CLANS)).build());
        assertThat(again.nextMatching(ServiceMessage::hasWelcome)).isNotNull();
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");
        assertThat(holders.watchersOf(CLANS)).containsExactly("lobby-01");

        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 3));
        assertThat(again.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
        apply("lobby-02", "tok-b", key(), store(CLANS, "green"), inc(SCORE, 3));
        assertThat(again.nextDocChanged().getRef()).isEqualTo(storeRow(CLANS, "green"));
    }

    @Test
    void nodeGoneRemovesHolders() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        Client b = connect("lobby-02", "tok-b");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        loadPlayer("lobby-02", "tok-b", p);
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);

        a.cancel();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(holders.holdersOf(p)).containsExactly("lobby-02");
            assertThat(holders.watchersOf(CLANS)).isEmpty();
        });
        // B still receives, nothing is queued for the gone node.
        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 1));
        assertThat(b.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
        b.cancel();
    }

    @Test
    void reconnectBeforeNodeGoneKeepsHolders() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        // R2: the new session supersedes the old one; the old session's end is no node-gone.
        Client again = connect("lobby-01", "tok-a");
        assertThat(a.status()).isEqualTo(Status.Code.ABORTED);
        server.nodes().serialize("lobby-01", () -> {}).get(5, TimeUnit.SECONDS);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");

        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 1));
        assertThat(again.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
    }

    @Test
    void shutdownRemovesNoHolders() throws Exception {
        start();
        connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        // R2: after NOT_SERVING nothing is removed.
        server.nodes().shutdown(Status.UNAVAILABLE);
        server.nodes().serialize("lobby-01", () -> {}).get(5, TimeUnit.SECONDS);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");
    }

    @Test
    void serverSwitchBothHoldersReceive() throws Exception {
        startDefault();
        Client a = connect("lobby-01", "tok-a");
        Client b = connect("lobby-02", "tok-b");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        loadPlayer("lobby-02", "tok-b", p);

        String key = key();
        ApplyResponse r = apply("lobby-01", "tok-a", key, player(p), inc(COINS, 70));
        long version = r.getRows(0).getVersion();

        DocChanged atB = b.nextDocChanged();
        assertThat(atB.getRef()).isEqualTo(playerRow(p, "economy"));
        assertThat(atB.getVersion()).isEqualTo(version);
        assertThat(atB.getDataMap()).containsEntry("coins", Value.newBuilder().setLongValue(70).build());
        DocChanged echo = a.nextDocChanged();
        assertThat(echo.getVersion()).isEqualTo(version);
        assertThat(echo.getIdempotencyKey()).isEqualTo(key);
        assertThat(atB.getIdempotencyKey()).isEqualTo(key);
    }

    // ---------- A-10 follow-up (review minors 1-3) ----------

    @Test
    void staleHeartbeatAfterReleaseDoesNotRestoreHolder() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);

        // Release and Unwatch at T0+1, then a heartbeat that was built before them still lists p and CLANS.
        clock.set(T0.plusSeconds(1));
        a.send(NodeMessage.newBuilder().setReleasePlayer(ReleasePlayer.newBuilder().setPlayer(p.toString())).build());
        a.send(NodeMessage.newBuilder().setUnwatchStore(UnwatchStore.newBuilder().setStore(CLANS)).build());
        processed(a); // the tombstones carry T0+1
        clock.set(T0.plusSeconds(2));
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().addHeldPlayers(p.toString())
                .addWatchedStores(CLANS)).build());
        processed(a);
        assertThat(holders.holdersOf(p)).isEmpty();
        assertThat(holders.watchersOf(CLANS)).isEmpty();

        // Exactly 10 s after the release the tombstone has ended: a heartbeat repairs a lost holder again.
        clock.set(T0.plusSeconds(11));
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().addHeldPlayers(p.toString())
                .addWatchedStores(CLANS)).build());
        processed(a);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");
        assertThat(holders.watchersOf(CLANS)).containsExactly("lobby-01");
    }

    @Test
    void explicitLoadAndWatchOverrideTombstone() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        clock.set(T0.plusSeconds(1));
        a.send(NodeMessage.newBuilder().setReleasePlayer(ReleasePlayer.newBuilder().setPlayer(p.toString())).build());
        a.send(NodeMessage.newBuilder().setUnwatchStore(UnwatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);
        assertThat(holders.holdersOf(p)).isEmpty();

        // A new LoadPlayer / WatchStore is a newer registration than the tombstone.
        clock.set(T0.plusSeconds(2));
        loadPlayer("lobby-01", "tok-a", p);
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");
        assertThat(holders.watchersOf(CLANS)).containsExactly("lobby-01");

        // 10 s after the re-registration (11 s after the release) a heartbeat listing both keeps them.
        clock.set(T0.plusSeconds(12));
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().addHeldPlayers(p.toString())
                .addWatchedStores(CLANS)).build());
        processed(a);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");
        assertThat(holders.watchersOf(CLANS)).containsExactly("lobby-01");
        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 1));
        assertThat(a.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
    }

    @Test
    void loadPlayerInNodeGoneGapKeepsHolder() throws Exception {
        start();
        Client a = connect("lobby-01", "tok-a");
        UUID old = UUID.randomUUID();
        UUID gap = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", old);
        a.send(NodeMessage.newBuilder().setWatchStore(WatchStore.newBuilder().setStore(CLANS)).build());
        processed(a);

        // Hold the lane so that node-gone runs only after the LoadPlayer in the gap.
        CountDownLatch gate = new CountDownLatch(1);
        server.nodes().serialize("lobby-01", () -> {
            try {
                gate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        clock.set(T0.plusSeconds(1));
        a.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> server.nodes().get("lobby-01").isEmpty());
        clock.set(T0.plusSeconds(2));
        loadPlayer("lobby-01", "tok-a", gap);
        gate.countDown();
        server.nodes().serialize("lobby-01", () -> {}).get(5, TimeUnit.SECONDS);

        assertThat(holders.holdersOf(old)).isEmpty();
        assertThat(holders.watchersOf(CLANS)).isEmpty();
        assertThat(holders.holdersOf(gap)).containsExactly("lobby-01");
    }

    private UUID goneWithGapHolder() throws Exception {
        Client a = connect("lobby-01", "tok-a");
        UUID gap = UUID.randomUUID();
        CountDownLatch gate = new CountDownLatch(1);
        server.nodes().serialize("lobby-01", () -> {
            try {
                gate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        clock.set(T0.plusSeconds(1));
        a.cancel();
        await().atMost(Duration.ofSeconds(5)).until(() -> server.nodes().get("lobby-01").isEmpty());
        clock.set(T0.plusSeconds(2));
        loadPlayer("lobby-01", "tok-a", gap);
        gate.countDown();
        server.nodes().serialize("lobby-01", () -> {}).get(5, TimeUnit.SECONDS);
        assertThat(holders.holdersOf(gap)).containsExactly("lobby-01");
        return gap;
    }

    @Test
    void gapHolderOfNodeThatNeverReturnsIsSwept() throws Exception {
        start();
        UUID gap = goneWithGapHolder();

        clock.set(T0.plusSeconds(2).plus(HolderRegistry.GRACE).minusMillis(1));
        holders.sweepOrphans();
        assertThat(holders.holdersOf(gap)).containsExactly("lobby-01"); // still within the grace

        clock.set(T0.plusSeconds(2).plus(HolderRegistry.GRACE));
        holders.sweepOrphans();
        assertThat(holders.holdersOf(gap)).isEmpty();
    }

    @Test
    void gapHolderOfReconnectedNodeIsNotSwept() throws Exception {
        start();
        UUID gap = goneWithGapHolder();
        connect("lobby-01", "tok-a");

        clock.set(T0.plusSeconds(2).plus(HolderRegistry.GRACE).plusSeconds(60));
        holders.sweepOrphans();
        assertThat(holders.holdersOf(gap)).containsExactly("lobby-01");
    }

    @Test
    void sixArgStartRoutesChanges() throws Exception {
        AgoraService service = new AgoraService();
        NodeRegistry nodes = new NodeRegistry();
        NodeMessageDispatcher dispatcher = new NodeMessageDispatcher();
        PresenceService presence = new PresenceService(nodes);
        presence.install(dispatcher);
        PresenceEndpoints.register(service, presence, nodes);
        List<UUID> claims = new java.util.concurrent.CopyOnWriteArrayList<>();
        server = AgoraServer.start(config(), service, (node, player) -> claims.add(player), nodes, dispatcher,
                presence);
        afterStart();
        assertRoutesAndForwardsClaims(claims);
    }

    @Test
    void threeArgStartRoutesChanges() throws Exception {
        List<UUID> claims = new java.util.concurrent.CopyOnWriteArrayList<>();
        server = AgoraServer.start(config(), new AgoraService(), (node, player) -> claims.add(player));
        afterStart();
        assertRoutesAndForwardsClaims(claims);
    }

    @Test
    void sixArgStartWithRegistryUsesIt() throws Exception {
        AgoraService service = new AgoraService();
        NodeRegistry nodes = new NodeRegistry();
        NodeMessageDispatcher dispatcher = new NodeMessageDispatcher();
        PresenceService presence = new PresenceService(nodes);
        presence.install(dispatcher);
        PresenceEndpoints.register(service, presence, nodes);
        holders = new HolderRegistry(nodes, clock); // not installed by the caller: the overload installs it
        server = AgoraServer.start(config(), service, holders, nodes, dispatcher, presence);
        afterStart();
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        assertThat(holders.holdersOf(p)).containsExactly("lobby-01");
        apply("lobby-02", "tok-b", key(), player(p), inc(COINS, 1));
        assertThat(a.nextDocChanged().getRef()).isEqualTo(playerRow(p, "economy"));
        // Installed on the dispatcher: a heartbeat reaches it.
        UUID q = UUID.randomUUID();
        a.send(NodeMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().addHeldPlayers(p.toString())
                .addHeldPlayers(q.toString())).build());
        processed(a);
        assertThat(holders.holdersOf(q)).containsExactly("lobby-01");
    }

    void assertRoutesAndForwardsClaims(List<UUID> claims) throws Exception {
        Client a = connect("lobby-01", "tok-a");
        UUID p = UUID.randomUUID();
        loadPlayer("lobby-01", "tok-a", p);
        assertThat(claims).containsExactly(p);
        String key = key();
        apply("lobby-02", "tok-b", key, player(p), inc(COINS, 4));
        DocChanged e = a.nextDocChanged();
        assertThat(e.getRef()).isEqualTo(playerRow(p, "economy"));
        assertThat(e.getIdempotencyKey()).isEqualTo(key);
    }

    // ---------- helpers ----------

    static String key() {
        return UUID.randomUUID().toString();
    }

    static DocRef player(UUID p) {
        return DocRef.newBuilder().setPlayer(p.toString()).build();
    }

    static DocRef store(String store, String id) {
        return DocRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore(store).setId(id)).build();
    }

    static RowRef playerRow(UUID p, String ns) {
        return RowRef.newBuilder().setPlayerRow(PlayerRow.newBuilder().setPlayer(p.toString()).setNamespace(ns)).build();
    }

    static RowRef storeRow(String store, String id) {
        return RowRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore(store).setId(id)).build();
    }

    static FieldOp inc(FieldSpec f, long by) {
        return FieldOp.newBuilder().setField(f).setInc(by).build();
    }

    Metadata headers(String node, String token) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), node);
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return md;
    }

    AgoraGrpc.AgoraBlockingStub stub(String node, String token) {
        return AgoraGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node, token)))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    void loadPlayer(String node, String token, UUID p) {
        stub(node, token).loadPlayer(LoadPlayerRequest.newBuilder().setPlayer(p.toString()).build());
    }

    ApplyResponse apply(String node, String token, String key, DocRef doc, FieldOp op) {
        return stub(node, token).apply(ApplyRequest.newBuilder().setIdempotencyKey(key).setDoc(doc).addOps(op)
                .setCreatedAtMillis(System.currentTimeMillis()).build());
    }

    /** Waits until the server has handled every message {@code c} sent so far (marker on the same lane). */
    void processed(Client c) throws Exception {
        String id = "marker-" + System.nanoTime();
        CompletableFuture<Void> seen = new CompletableFuture<>();
        markers.put(id, seen);
        c.send(NodeMessage.newBuilder().setKickResult(KickResult.newBuilder().setRequestId(id)).build());
        seen.get(5, TimeUnit.SECONDS);
    }

    Client client(String node, String token) {
        Client c = new Client();
        AgoraGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node, token)))
                .connect(c);
        clients.add(c);
        return c;
    }

    Client connect(String node, String token) throws Exception {
        Client c = client(node, token);
        c.send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId(node).setKind(NodeKind.PAPER)).build());
        assertThat(c.nextMatching(ServiceMessage::hasWelcome)).isNotNull();
        return c;
    }

    static final class Client implements ClientResponseObserver<NodeMessage, ServiceMessage> {
        final LinkedBlockingQueue<ServiceMessage> received = new LinkedBlockingQueue<>();
        final CompletableFuture<Status> closed = new CompletableFuture<>();
        volatile ClientCallStreamObserver<NodeMessage> requests;

        @Override public void beforeStart(ClientCallStreamObserver<NodeMessage> r) { this.requests = r; }
        @Override public void onNext(ServiceMessage value) { received.add(value); }
        @Override public void onError(Throwable t) { closed.complete(Status.fromThrowable(t)); }
        @Override public void onCompleted() { closed.complete(Status.OK); }

        synchronized void send(NodeMessage m) {
            requests.onNext(m);
        }

        void cancel() {
            if (requests != null && !closed.isDone()) requests.cancel("test done", null);
        }

        Status.Code status() throws Exception {
            return closed.get(5, TimeUnit.SECONDS).getCode();
        }

        ServiceMessage nextMatching(Predicate<ServiceMessage> p) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < until) {
                ServiceMessage m = received.poll(until - System.nanoTime(), TimeUnit.NANOSECONDS);
                if (m == null) break;
                if (p.test(m)) return m;
            }
            throw new AssertionError("no matching message within 5 s");
        }

        DocChanged nextDocChanged() throws InterruptedException {
            return nextMatching(ServiceMessage::hasDocChanged).getDocChanged();
        }

        /** Every DocChanged up to and including the first matching {@code last}. */
        List<DocChanged> docChangedUntil(Predicate<DocChanged> last) throws InterruptedException {
            List<DocChanged> seen = new ArrayList<>();
            while (true) {
                DocChanged e = nextDocChanged();
                seen.add(e);
                if (last.test(e)) return seen;
            }
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

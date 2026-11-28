package de.kalypzo.agora.teleport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import de.kalypzo.agora.config.AgoraConfig.TeleportTimeouts;
import de.kalypzo.agora.proto.v1.ArrivalFailed;
import de.kalypzo.agora.proto.v1.ArrivalPrepared;
import de.kalypzo.agora.proto.v1.ArrivalStatus;
import de.kalypzo.agora.proto.v1.CancelArrival;
import de.kalypzo.agora.proto.v1.ConnectPlayer;
import de.kalypzo.agora.proto.v1.ConnectResult;
import de.kalypzo.agora.proto.v1.ConnectStatus;
import de.kalypzo.agora.proto.v1.NetworkLocation;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PlayerArrived;
import de.kalypzo.agora.proto.v1.PrepareArrival;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.TeleportRequest;
import de.kalypzo.agora.proto.v1.TeleportResponse;
import de.kalypzo.agora.proto.v1.TeleportStatus;
import de.kalypzo.agora.testing.InProcessAgora;
import de.kalypzo.agora.testing.InProcessAgora.Client;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** T-3: the basic network teleport flow (PrepareArrival, ConnectPlayer, PlayerArrived) and its time limits. */
class TeleportServiceTest {

    static final String STEVE = "00000000-0000-0000-0000-000000000001";
    static final String ALEX = "00000000-0000-0000-0000-000000000002";
    static final NetworkLocation WHERE2 = NetworkLocation.newBuilder().setServer("lobby-02").setWorld("world")
            .setX(1.5).setY(64).setZ(-3).setYaw(90).setPitch(10).build();
    static final NetworkLocation WHERE1 = WHERE2.toBuilder().setServer("lobby-01").build();
    static final TeleportTimeouts FAST = new TeleportTimeouts(
            Duration.ofMillis(300), Duration.ofMillis(800), Duration.ofMillis(1200), Duration.ofSeconds(3));

    @TempDir Path dir;
    InProcessAgora agora;
    Client proxy;
    Client lobby1;
    Client lobby2;

    @AfterEach
    void tearDown() {
        if (agora != null) agora.close();
    }

    @Test
    void happyPathOk() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        PrepareArrival p = prepareAt(lobby2);
        assertThat(p.getRequestId()).isNotBlank();
        assertThat(p.getPlayer()).isEqualTo(STEVE);
        assertThat(p.getLocation()).isEqualTo(WHERE2);
        prepared(lobby2, p.getRequestId(), ArrivalStatus.READY);

        ConnectPlayer c = proxy.nextMatching(ServiceMessage::hasConnectPlayer).getConnectPlayer();
        assertThat(c.getRequestId()).isEqualTo(p.getRequestId());
        assertThat(c.getPlayer()).isEqualTo(STEVE);
        assertThat(c.getServer()).isEqualTo("lobby-02");
        connectResult(p.getRequestId(), ConnectStatus.CONNECTED);
        arrived(lobby2, p.getRequestId());

        assertThat(status(f)).isEqualTo(TeleportStatus.OK);
        noCancelBeforeNextPrepare();
    }

    @Test
    void arrivalBeforeConnectResultIsOk() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = readyAndConnect();
        arrived(lobby2, id);
        assertThat(status(f)).isEqualTo(TeleportStatus.OK);

        // The late FAILED changes nothing.
        connectResult(id, ConnectStatus.FAILED);
        agora.processed(proxy);
        noCancelBeforeNextPrepare();
    }

    @Test
    void connectResultFirstWaitsForArrival() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = readyAndConnect();
        connectResult(id, ConnectStatus.CONNECTED);
        agora.processed(proxy);
        assertThat(f).isNotDone();
        arrived(lobby2, id);
        assertThat(status(f)).isEqualTo(TeleportStatus.OK);
    }

    @Test
    void alreadyConnectedWaitsForArrival() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = readyAndConnect();
        connectResult(id, ConnectStatus.ALREADY_CONNECTED);
        agora.processed(proxy);
        assertThat(f).isNotDone();
        arrived(lobby2, id);
        assertThat(status(f)).isEqualTo(TeleportStatus.OK);
    }

    @Test
    void worldNotFound() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        prepared(lobby2, prepareAt(lobby2).getRequestId(), ArrivalStatus.ARRIVAL_WORLD_NOT_FOUND);
        assertThat(status(f)).isEqualTo(TeleportStatus.WORLD_NOT_FOUND);
        noConnectBeforeNext(ServiceMessage::hasConnectPlayer);
    }

    @Test
    void unspecifiedArrivalIsUnavailable() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        prepared(lobby2, id, ArrivalStatus.ARRIVAL_STATUS_UNSPECIFIED);
        assertThat(status(f)).isEqualTo(TeleportStatus.SERVER_UNAVAILABLE);
        CancelArrival cancel = cancelAt(lobby2);
        assertThat(cancel.getRequestId()).isEqualTo(id);
        assertThat(cancel.getPlayer()).isEqualTo(STEVE);
    }

    @Test
    void alreadyHereSkipsConnect() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        prepared(lobby2, id, ArrivalStatus.ALREADY_HERE);
        agora.processed(lobby2);
        assertThat(f).isNotDone();
        arrived(lobby2, id);
        assertThat(status(f)).isEqualTo(TeleportStatus.OK);
        noConnectBeforeNext(ServiceMessage::hasConnectPlayer);
    }

    @Test
    void arrivalBeforePreparedReplyIsOk() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        arrived(lobby2, id);
        // The late READY falls under AR-12: no ConnectPlayer for an ended teleport.
        prepared(lobby2, id, ArrivalStatus.READY);
        assertThat(status(f)).isEqualTo(TeleportStatus.OK);
        agora.processed(lobby2);
        noConnectBeforeNext(ServiceMessage::hasConnectPlayer);
    }

    @Test
    void connectPreparesEvenIfPresenceSaysHere() throws Exception {
        start(entry(STEVE, "lobby-02"));
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", null);
        PrepareArrival p = prepareAt(lobby2);
        assertThat(p.hasLocation()).isFalse();
        prepared(lobby2, p.getRequestId(), ArrivalStatus.ALREADY_HERE);
        arrived(lobby2, p.getRequestId());
        assertThat(status(f)).isEqualTo(TeleportStatus.OK);
    }

    @Test
    void proxyFailedIsConnectFailedAndCancels() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = readyAndConnect();
        connectResult(id, ConnectStatus.FAILED);
        assertThat(status(f)).isEqualTo(TeleportStatus.CONNECT_FAILED);
        assertThat(cancelAt(lobby2).getRequestId()).isEqualTo(id);
    }

    @Test
    void playerNotFoundAtProxyIsNotOnline() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        connectResult(readyAndConnect(), ConnectStatus.PLAYER_NOT_FOUND);
        assertThat(status(f)).isEqualTo(TeleportStatus.NOT_ONLINE);
    }

    @Test
    void arrivalFailedIsConnectFailed() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = readyAndConnect();
        lobby2.send(NodeMessage.newBuilder().setArrivalFailed(ArrivalFailed.newBuilder().setRequestId(id)).build());
        assertThat(status(f)).isEqualTo(TeleportStatus.CONNECT_FAILED);
    }

    @Test
    void offlineIsNotOnline() throws Exception {
        start();
        assertThat(status(teleport(ALEX, "lobby-02", WHERE2))).isEqualTo(TeleportStatus.NOT_ONLINE);
        noPrepareBeforeNext(STEVE);
    }

    @Test
    void noServerYetIsNotOnline() throws Exception {
        start(entry(STEVE, ""));
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-01"));
        assertThat(status(teleport(STEVE, "lobby-02", WHERE2))).isEqualTo(TeleportStatus.NOT_ONLINE);
        noPrepareBeforeNext(ALEX);
    }

    @Test
    void unknownTargetIsUnavailable() throws Exception {
        start();
        assertThat(status(teleport(STEVE, "lobby-09", null))).isEqualTo(TeleportStatus.SERVER_UNAVAILABLE);
    }

    @Test
    void velocityTargetIsUnavailable() throws Exception {
        start();
        assertThat(status(teleport(STEVE, "proxy-01", null))).isEqualTo(TeleportStatus.SERVER_UNAVAILABLE);
        noConnectBeforeNext(ServiceMessage::hasPrepareArrival);
    }

    @Test
    void prepareTimeoutCancels() throws Exception {
        start();
        long t0 = System.nanoTime();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        assertThat(status(f)).isEqualTo(TeleportStatus.TIMEOUT);
        assertThat(Duration.ofNanos(System.nanoTime() - t0))
                .isBetween(Duration.ofMillis(250), Duration.ofSeconds(2));
        assertThat(cancelAt(lobby2).getRequestId()).isEqualTo(id);
    }

    @Test
    void connectTimeout() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        long ready = System.nanoTime();
        prepared(lobby2, id, ArrivalStatus.READY);
        proxy.nextMatching(ServiceMessage::hasConnectPlayer);
        assertThat(status(f)).isEqualTo(TeleportStatus.TIMEOUT);
        // The connect timer (800 ms), not the arrival timer (1200 ms).
        assertThat(Duration.ofNanos(System.nanoTime() - ready)).isLessThan(Duration.ofMillis(1100));
        assertThat(cancelAt(lobby2).getRequestId()).isEqualTo(id);
    }

    @Test
    void arrivalTimeout() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        long ready = System.nanoTime();
        prepared(lobby2, id, ArrivalStatus.READY);
        proxy.nextMatching(ServiceMessage::hasConnectPlayer);
        connectResult(id, ConnectStatus.CONNECTED);
        assertThat(status(f)).isEqualTo(TeleportStatus.TIMEOUT);
        // CONNECTED stopped the connect timer (800 ms): the arrival timer (1200 ms) ended it.
        assertThat(Duration.ofNanos(System.nanoTime() - ready)).isGreaterThanOrEqualTo(Duration.ofMillis(1100));
    }

    @Test
    void lateArrivalAfterTimeoutIgnored() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        assertThat(status(f)).isEqualTo(TeleportStatus.TIMEOUT);
        arrived(lobby2, id);
        agora.processed(lobby2);

        CompletableFuture<TeleportResponse> second = teleport(STEVE, "lobby-02", WHERE2);
        String id2 = readyAndConnect();
        assertThat(id2).isNotEqualTo(id);
        connectResult(id2, ConnectStatus.CONNECTED);
        arrived(lobby2, id2);
        assertThat(status(second)).isEqualTo(TeleportStatus.OK);
    }

    @Test
    void answerFromOtherSessionIgnored() throws Exception {
        start();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        prepared(lobby1, prepareAt(lobby2).getRequestId(), ArrivalStatus.READY);
        assertThat(status(f)).isEqualTo(TeleportStatus.TIMEOUT);
        noConnectBeforeNext(ServiceMessage::hasConnectPlayer);
    }

    @Test
    void velocityCallerDenied() throws Exception {
        start();
        StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class,
                () -> agora.blockingStub("proxy-01").teleport(request(STEVE, "lobby-02", WHERE2)));
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    @Test
    void serverMismatchInvalid() throws Exception {
        start();
        StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class,
                () -> agora.blockingStub("lobby-01").teleport(request(STEVE, "lobby-02", WHERE1)));
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    void start() throws Exception {
        start(entry(STEVE, "lobby-01"));
    }

    void start(PresenceEntry steve) throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3), FAST);
        proxy = agora.connect("proxy-01", NodeKind.VELOCITY, steve);
        lobby1 = agora.connect("lobby-01", NodeKind.PAPER);
        lobby2 = agora.connect("lobby-02", NodeKind.PAPER);
    }

    /** lobby-02 answers READY to the PrepareArrival; returns the request id once the proxy got its ConnectPlayer. */
    String readyAndConnect() throws Exception {
        String id = prepareAt(lobby2).getRequestId();
        prepared(lobby2, id, ArrivalStatus.READY);
        assertThat(proxy.nextMatching(ServiceMessage::hasConnectPlayer).getConnectPlayer().getRequestId())
                .isEqualTo(id);
        return id;
    }

    /**
     * FIFO barrier on lobby-02: a new teleport's PrepareArrival must arrive before any CancelArrival, since a Cancel
     * of the earlier teleport would have been sent on the same session first.
     */
    void noCancelBeforeNextPrepare() throws Exception {
        teleport(STEVE, "lobby-02", WHERE2);
        assertThat(lobby2.nextMatching(m -> m.hasCancelArrival() || m.hasPrepareArrival()).hasPrepareArrival())
                .isTrue();
    }

    /** FIFO barrier on lobby-02: the next PrepareArrival is the one of a new teleport of {@code barrier}. */
    void noPrepareBeforeNext(String barrier) throws Exception {
        teleport(barrier, "lobby-02", WHERE2);
        assertThat(prepareAt(lobby2).getPlayer()).isEqualTo(barrier);
    }

    /**
     * FIFO barrier on the proxy: a new teleport's ConnectPlayer must be the first message matching
     * {@code unwanted} or ConnectPlayer.
     */
    void noConnectBeforeNext(Predicate<ServiceMessage> unwanted) throws Exception {
        teleport(STEVE, "lobby-02", WHERE2);
        String id = prepareAt(lobby2).getRequestId();
        prepared(lobby2, id, ArrivalStatus.READY);
        ServiceMessage m = proxy.nextMatching(x -> unwanted.test(x) || x.hasConnectPlayer());
        assertThat(m.hasConnectPlayer()).isTrue();
        assertThat(m.getConnectPlayer().getRequestId()).isEqualTo(id);
    }

    static PrepareArrival prepareAt(Client c) throws Exception {
        return c.nextMatching(ServiceMessage::hasPrepareArrival).getPrepareArrival();
    }

    static CancelArrival cancelAt(Client c) throws Exception {
        return c.nextMatching(ServiceMessage::hasCancelArrival).getCancelArrival();
    }

    static void prepared(Client c, String id, ArrivalStatus s) {
        c.send(NodeMessage.newBuilder().setArrivalPrepared(ArrivalPrepared.newBuilder().setRequestId(id).setStatus(s))
                .build());
    }

    static void arrived(Client c, String id) {
        c.send(NodeMessage.newBuilder().setPlayerArrived(PlayerArrived.newBuilder().setRequestId(id)).build());
    }

    void connectResult(String id, ConnectStatus s) {
        proxy.send(NodeMessage.newBuilder().setConnectResult(ConnectResult.newBuilder().setRequestId(id).setStatus(s))
                .build());
    }

    static PresenceEntry entry(String player, String server) {
        return PresenceEntry.newBuilder().setPlayer(player).setName("Steve").setServer(server).setLocale("de_DE")
                .build();
    }

    static TeleportRequest request(String player, String server, NetworkLocation location) {
        TeleportRequest.Builder b = TeleportRequest.newBuilder().setPlayer(player).setServer(server);
        if (location != null) b.setLocation(location);
        return b.build();
    }

    static TeleportStatus status(CompletableFuture<TeleportResponse> f) throws Exception {
        return f.get(5, TimeUnit.SECONDS).getStatus();
    }

    /** Calls {@code Teleport} as lobby-01. */
    CompletableFuture<TeleportResponse> teleport(String player, String server, NetworkLocation location) {
        CompletableFuture<TeleportResponse> f = new CompletableFuture<>();
        agora.stub("lobby-01").withDeadlineAfter(10, TimeUnit.SECONDS).teleport(request(player, server, location),
                new StreamObserver<>() {
                    @Override public void onNext(TeleportResponse r) { f.complete(r); }
                    @Override public void onError(Throwable t) { f.completeExceptionally(t); }
                    @Override public void onCompleted() {}
                });
        return f;
    }
}

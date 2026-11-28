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
import de.kalypzo.agora.proto.v1.PlayerLeft;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T-3: the basic network teleport flow (PrepareArrival, ConnectPlayer, PlayerArrived) and its time limits. T-4:
 * superseding, waiting for an open ConnectPlayer, node loss, presence removal, the total limit and concurrency.
 */
class TeleportServiceTest {

    static final String STEVE = "00000000-0000-0000-0000-000000000001";
    static final String ALEX = "00000000-0000-0000-0000-000000000002";
    static final NetworkLocation WHERE2 = NetworkLocation.newBuilder().setServer("lobby-02").setWorld("world")
            .setX(1.5).setY(64).setZ(-3).setYaw(90).setPitch(10).build();
    static final NetworkLocation WHERE1 = WHERE2.toBuilder().setServer("lobby-01").build();
    static final NetworkLocation WHERE3 = WHERE2.toBuilder().setX(7).build();
    static final TeleportTimeouts FAST = new TeleportTimeouts(
            Duration.ofMillis(300), Duration.ofMillis(800), Duration.ofMillis(1200), Duration.ofSeconds(3));
    /** No timer fires while a test checks what is (not) sent during the wait for an open ConnectPlayer. */
    static final TeleportTimeouts LONG = new TeleportTimeouts(
            Duration.ofSeconds(2), Duration.ofSeconds(3), Duration.ofSeconds(3), Duration.ofSeconds(4));

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
        noPrepareBeforeNext(lobby2, "lobby-02", STEVE);
    }

    @Test
    void noServerYetIsNotOnline() throws Exception {
        start(entry(STEVE, ""));
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-01"));
        assertThat(status(teleport(STEVE, "lobby-02", WHERE2))).isEqualTo(TeleportStatus.NOT_ONLINE);
        noPrepareBeforeNext(lobby2, "lobby-02", ALEX);
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

    @Test
    void newerTeleportSupersedes() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-02"));
        CompletableFuture<TeleportResponse> t1 = teleport(STEVE, "lobby-02", WHERE2);
        String id1 = readyAndConnect();
        CompletableFuture<TeleportStatus> t2 = direct(STEVE, "lobby-01", WHERE1);
        assertThat(status(t1)).isEqualTo(TeleportStatus.SUPERSEDED);
        assertThat(cancelAt(lobby2).getRequestId()).isEqualTo(id1);
        agora.processed(proxy);
        // T2 waits for the ConnectResult of T1: no PrepareArrival at lobby-01 yet.
        noPrepareBeforeNext(lobby1, "lobby-01", ALEX);

        connectResult(id1, ConnectStatus.CONNECTED);
        PrepareArrival p2 = prepareAt(lobby1);
        assertThat(p2.getPlayer()).isEqualTo(STEVE);
        assertThat(p2.getLocation()).isEqualTo(WHERE1);
        prepared(lobby1, p2.getRequestId(), ArrivalStatus.READY);
        ConnectPlayer c2 = proxy.nextMatching(ServiceMessage::hasConnectPlayer).getConnectPlayer();
        assertThat(c2.getRequestId()).isEqualTo(p2.getRequestId());
        assertThat(c2.getServer()).isEqualTo("lobby-01");
        connectResult(p2.getRequestId(), ConnectStatus.CONNECTED);
        arrived(lobby1, p2.getRequestId());
        assertThat(t2.get(5, TimeUnit.SECONDS)).isEqualTo(TeleportStatus.OK);
    }

    @Test
    void openConnectAfterOkBlocksNext() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-01"));
        CompletableFuture<TeleportResponse> t1 = teleport(STEVE, "lobby-02", WHERE2);
        String id1 = readyAndConnect();
        arrived(lobby2, id1);
        assertThat(status(t1)).isEqualTo(TeleportStatus.OK);

        CompletableFuture<TeleportStatus> t2 = direct(STEVE, "lobby-02", WHERE3);
        noPrepareBeforeNext(lobby2, "lobby-02", ALEX);
        connectResult(id1, ConnectStatus.CONNECTED);
        PrepareArrival p2 = prepareAt(lobby2);
        assertThat(p2.getPlayer()).isEqualTo(STEVE);
        assertThat(p2.getLocation()).isEqualTo(WHERE3);
        assertThat(t2).isNotDone();
    }

    @Test
    void openConnectExpires() throws Exception {
        start();
        CompletableFuture<TeleportResponse> t1 = teleport(STEVE, "lobby-02", WHERE2);
        String id1 = prepareAt(lobby2).getRequestId();
        long ready = System.nanoTime();
        prepared(lobby2, id1, ArrivalStatus.READY);
        proxy.nextMatching(ServiceMessage::hasConnectPlayer);
        arrived(lobby2, id1);
        assertThat(status(t1)).isEqualTo(TeleportStatus.OK);

        direct(STEVE, "lobby-02", WHERE3);
        PrepareArrival p2 = prepareAt(lobby2);
        assertThat(p2.getLocation()).isEqualTo(WHERE3);
        // The open ConnectPlayer of T1 expired after the connect limit (800 ms).
        assertThat(Duration.ofNanos(System.nanoTime() - ready))
                .isBetween(Duration.ofMillis(700), Duration.ofSeconds(2));
    }

    @Test
    void thirdTeleportWhileWaiting() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-02"));
        CompletableFuture<TeleportResponse> t1 = teleport(STEVE, "lobby-02", WHERE2);
        String id1 = readyAndConnect();
        CompletableFuture<TeleportStatus> t2 = direct(STEVE, "lobby-01", WHERE1);
        assertThat(status(t1)).isEqualTo(TeleportStatus.SUPERSEDED);
        assertThat(cancelAt(lobby2).getRequestId()).isEqualTo(id1);
        CompletableFuture<TeleportStatus> t3 = direct(STEVE, "lobby-02", WHERE3);
        assertThat(t2.get(5, TimeUnit.SECONDS)).isEqualTo(TeleportStatus.SUPERSEDED);

        connectResult(id1, ConnectStatus.CONNECTED);
        PrepareArrival p3 = prepareAt(lobby2);
        assertThat(p3.getPlayer()).isEqualTo(STEVE);
        assertThat(p3.getLocation()).isEqualTo(WHERE3);
        assertThat(t3).isNotDone();
        // T2 sent neither a PrepareArrival nor a CancelArrival to lobby-01.
        noPrepareBeforeNext(lobby1, "lobby-01", ALEX);
    }

    /** A teleport that ended before its PrepareArrival went out sends none (it would come after its own Cancel). */
    @Test
    void endedBeforeSendSendsNoPrepare() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-01"));
        CompletableFuture<TeleportStatus> t1 = direct(STEVE, "lobby-02", WHERE2);
        String id1 = prepareAt(lobby2).getRequestId();
        // T1 completes among the actions of T2's transition, before T2's PrepareArrival; T3 then supersedes T2.
        t1.thenRun(() -> direct(STEVE, "lobby-02", WHERE3));
        CompletableFuture<TeleportStatus> t2 = direct(STEVE, "lobby-02", WHERE2.toBuilder().setX(5).build());
        assertThat(t2.get(5, TimeUnit.SECONDS)).isEqualTo(TeleportStatus.SUPERSEDED);

        Predicate<ServiceMessage> arrival = m -> m.hasPrepareArrival() || m.hasCancelArrival();
        assertThat(lobby2.nextMatching(arrival).getCancelArrival().getRequestId()).isEqualTo(id1);
        assertThat(lobby2.nextMatching(arrival).hasCancelArrival()).isTrue(); // T2's
        assertThat(lobby2.nextMatching(arrival).getPrepareArrival().getLocation()).isEqualTo(WHERE3);
        noPrepareBeforeNext(lobby2, "lobby-02", ALEX);
    }

    @Test
    void targetGoneIsUnavailable() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        prepareAt(lobby2);
        lobby2.cancel();
        assertThat(status(f)).isEqualTo(TeleportStatus.SERVER_UNAVAILABLE);
    }

    @Test
    void proxyGoneIsNotOnline() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "lobby-01"));
        CompletableFuture<TeleportResponse> t1 = teleport(STEVE, "lobby-02", WHERE2);
        readyAndConnect();
        proxy.cancel();
        assertThat(status(t1)).isEqualTo(TeleportStatus.NOT_ONLINE);

        // The open ConnectPlayer went with the proxy: the next teleport starts at once (FIFO before ALEX's).
        proxy = agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "lobby-01"));
        direct(STEVE, "lobby-02", WHERE3);
        teleport(ALEX, "lobby-02", null);
        PrepareArrival p2 = prepareAt(lobby2);
        assertThat(p2.getPlayer()).isEqualTo(STEVE);
        assertThat(p2.getLocation()).isEqualTo(WHERE3);
    }

    @Test
    void presenceRemovedIsNotOnline() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        String id = readyAndConnect();
        left(STEVE);
        assertThat(status(f)).isEqualTo(TeleportStatus.NOT_ONLINE);
        assertThat(cancelAt(lobby2).getRequestId()).isEqualTo(id);
    }

    @Test
    void presenceRemovedWhileWaiting() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        CompletableFuture<TeleportResponse> t1 = teleport(STEVE, "lobby-02", WHERE2);
        readyAndConnect();
        CompletableFuture<TeleportStatus> t2 = direct(STEVE, "lobby-01", WHERE1);
        assertThat(status(t1)).isEqualTo(TeleportStatus.SUPERSEDED);
        left(STEVE);
        assertThat(t2.get(5, TimeUnit.SECONDS)).isEqualTo(TeleportStatus.NOT_ONLINE);
    }

    @Test
    void totalTimeout() throws Exception {
        start(entry(STEVE, "lobby-01"), new TeleportTimeouts(
                Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofMillis(500)));
        long t0 = System.nanoTime();
        CompletableFuture<TeleportResponse> f = teleport(STEVE, "lobby-02", WHERE2);
        prepareAt(lobby2);
        assertThat(status(f)).isEqualTo(TeleportStatus.TIMEOUT);
        assertThat(Duration.ofNanos(System.nanoTime() - t0))
                .isBetween(Duration.ofMillis(400), Duration.ofMillis(900));
    }

    @Test
    void concurrentConnectResultAndArrival() throws Exception {
        start(entry(STEVE, "lobby-01"), LONG);
        ExecutorService two = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                CompletableFuture<TeleportStatus> f = direct(STEVE, "lobby-02", WHERE2);
                // FIFO: a CancelArrival of the previous round would come before this PrepareArrival.
                ServiceMessage m = lobby2.nextMatching(x -> x.hasCancelArrival() || x.hasPrepareArrival());
                assertThat(m.hasPrepareArrival()).isTrue();
                String id = m.getPrepareArrival().getRequestId();
                prepared(lobby2, id, ArrivalStatus.READY);
                assertThat(proxy.nextMatching(ServiceMessage::hasConnectPlayer).getConnectPlayer().getRequestId())
                        .isEqualTo(id);
                CountDownLatch go = new CountDownLatch(1);
                Future<?> a = two.submit(() -> {
                    go.await();
                    connectResult(id, ConnectStatus.CONNECTED);
                    return null;
                });
                Future<?> b = two.submit(() -> {
                    go.await();
                    arrived(lobby2, id);
                    return null;
                });
                go.countDown();
                a.get(5, TimeUnit.SECONDS);
                b.get(5, TimeUnit.SECONDS);
                assertThat(f.get(5, TimeUnit.SECONDS)).as("round %d", i).isEqualTo(TeleportStatus.OK);
            }
        } finally {
            two.shutdownNow();
        }
        noCancelBeforeNextPrepare();
    }

    void start() throws Exception {
        start(entry(STEVE, "lobby-01"));
    }

    void start(PresenceEntry steve) throws Exception {
        start(steve, FAST);
    }

    void start(PresenceEntry steve, TeleportTimeouts timeouts) throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3), timeouts);
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

    /**
     * FIFO barrier on {@code at} (node {@code server}): the next PrepareArrival or CancelArrival is the PrepareArrival
     * of a new teleport of {@code barrier}.
     */
    void noPrepareBeforeNext(Client at, String server, String barrier) throws Exception {
        teleport(barrier, server, null);
        ServiceMessage m = at.nextMatching(x -> x.hasPrepareArrival() || x.hasCancelArrival());
        assertThat(m.hasPrepareArrival()).isTrue();
        assertThat(m.getPrepareArrival().getPlayer()).isEqualTo(barrier);
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

    void left(String player) {
        proxy.send(NodeMessage.newBuilder().setPlayerLeft(PlayerLeft.newBuilder().setPlayer(player)).build());
    }

    /** Calls the service directly: when it returns, the transition and its sends have run. */
    CompletableFuture<TeleportStatus> direct(String player, String server, NetworkLocation location) {
        return agora.teleport.teleport(UUID.fromString(player), server, location);
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

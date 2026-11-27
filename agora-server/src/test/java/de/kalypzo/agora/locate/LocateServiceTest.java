package de.kalypzo.agora.locate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import de.kalypzo.agora.proto.v1.LocateRequest;
import de.kalypzo.agora.proto.v1.LocateResponse;
import de.kalypzo.agora.proto.v1.LocationReply;
import de.kalypzo.agora.proto.v1.LocationRequest;
import de.kalypzo.agora.proto.v1.NetworkLocation;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.testing.InProcessAgora;
import de.kalypzo.agora.testing.InProcessAgora.Client;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A-12: Locate (E1) asks the server node of the player by LocationRequest and waits for its LocationReply. */
class LocateServiceTest {

    static final String STEVE = "00000000-0000-0000-0000-000000000001";
    static final String ALEX = "00000000-0000-0000-0000-000000000002";
    static final NetworkLocation WHERE = NetworkLocation.newBuilder().setServer("lobby-01").setWorld("world")
            .setX(1.5).setY(64).setZ(-3).setYaw(90).setPitch(10).build();

    @TempDir Path dir;
    InProcessAgora agora;

    @AfterEach
    void tearDown() {
        if (agora != null) agora.close();
    }

    @Test
    void locateAsksServerNodeOfPlayer() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client server = agora.connect("lobby-01", NodeKind.PAPER);
        Client other = agora.connect("lobby-02", NodeKind.PAPER);

        CompletableFuture<LocateResponse> answer = locate("lobby-02", STEVE);
        LocationRequest asked = server.nextMatching(ServiceMessage::hasLocationRequest).getLocationRequest();
        assertThat(asked.getPlayer()).isEqualTo(STEVE);
        assertThat(asked.getRequestId()).isNotBlank();
        assertThat(answer).isNotDone();
        server.send(NodeMessage.newBuilder().setLocationReply(LocationReply.newBuilder()
                .setRequestId(asked.getRequestId()).setFound(true).setLocation(WHERE)).build());

        LocateResponse r = answer.get(5, TimeUnit.SECONDS);
        assertThat(r.getFound()).isTrue();
        assertThat(r.getLocation()).isEqualTo(WHERE);
        assertThat(other.drained(ServiceMessage::hasLocationRequest)).isEmpty();
    }

    @Test
    void locateOfflineReturnsNotFound() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client server = agora.connect("lobby-01", NodeKind.PAPER);

        // Not in the presence: no LocationRequest, found = false.
        assertThat(agora.blockingStub("lobby-02").locate(request(ALEX)).getFound()).isFalse();
        assertThat(server.drained(ServiceMessage::hasLocationRequest)).isEmpty();

        // The server node answers found = false (e.g. the player just left it): found = false.
        CompletableFuture<LocateResponse> answer = locate("lobby-02", STEVE);
        LocationRequest asked = server.nextMatching(ServiceMessage::hasLocationRequest).getLocationRequest();
        server.send(NodeMessage.newBuilder().setLocationReply(LocationReply.newBuilder()
                .setRequestId(asked.getRequestId()).setFound(false)).build());
        assertThat(answer.get(5, TimeUnit.SECONDS).getFound()).isFalse();
    }

    @Test
    void locateTimesOutUnavailable() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofMillis(200));
        agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client server = agora.connect("lobby-01", NodeKind.PAPER);

        long start = System.nanoTime();
        CompletableFuture<LocateResponse> answer = locate("lobby-02", STEVE);
        LocationRequest asked = server.nextMatching(ServiceMessage::hasLocationRequest).getLocationRequest();
        // The wait does not block the server node's lane: its messages are handled meanwhile.
        agora.processed(server);
        assertThat(answer).isNotDone();

        ExecutionException e = catchThrowableOfType(ExecutionException.class, () -> answer.get(5, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(e.getCause()).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));

        // A late reply matches no pending request any more and changes nothing.
        server.send(NodeMessage.newBuilder().setLocationReply(LocationReply.newBuilder()
                .setRequestId(asked.getRequestId()).setFound(true).setLocation(WHERE)).build());
        agora.processed(server);
    }

    @Test
    void locateWithoutServerSessionUnavailable() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));

        StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class,
                () -> agora.blockingStub("lobby-02").locate(request(STEVE)));
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void locateBeforeFirstSwitchReturnsNotFound() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", ""));

        assertThat(agora.blockingStub("lobby-02").locate(request(STEVE)).getFound()).isFalse();
    }

    @Test
    void serverGoneWhileWaitingIsUnavailable() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client server = agora.connect("lobby-01", NodeKind.PAPER);

        long start = System.nanoTime();
        CompletableFuture<LocateResponse> answer = locate("lobby-02", STEVE);
        server.nextMatching(ServiceMessage::hasLocationRequest);
        server.cancel();

        ExecutionException e = catchThrowableOfType(ExecutionException.class, () -> answer.get(5, TimeUnit.SECONDS));
        assertThat(Status.fromThrowable(e.getCause()).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
    }

    @Test
    void invalidPlayerIsInvalidArgument() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class,
                () -> agora.blockingStub("lobby-02").locate(request("not-a-uuid")));
        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    // ---------- helpers ----------

    static PresenceEntry entry(String player, String name, String server) {
        return PresenceEntry.newBuilder().setPlayer(player).setName(name).setServer(server).setLocale("de_DE").build();
    }

    static LocateRequest request(String player) {
        return LocateRequest.newBuilder().setPlayer(player).build();
    }

    CompletableFuture<LocateResponse> locate(String node, String player) {
        CompletableFuture<LocateResponse> f = new CompletableFuture<>();
        agora.stub(node).locate(request(player), new StreamObserver<>() {
            @Override public void onNext(LocateResponse r) { f.complete(r); }
            @Override public void onError(Throwable t) { f.completeExceptionally(t); }
            @Override public void onCompleted() {}
        });
        return f;
    }
}

package de.kalypzo.agora;

import static org.assertj.core.api.Assertions.assertThat;

import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.ApplyRequest;
import de.kalypzo.agora.proto.v1.ClaimPresenceRequest;
import de.kalypzo.agora.proto.v1.Delivery;
import de.kalypzo.agora.proto.v1.DeliveryKind;
import de.kalypzo.agora.proto.v1.DocRef;
import de.kalypzo.agora.proto.v1.FieldOp;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.LoadPlayerRequest;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.SendDelivery;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.Value;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-14: the real server against Postgres with raw gRPC clients as proxy-01 and lobby-01. */
class AgoraEndToEndIT {

    static final FieldSpec COINS = FieldSpec.newBuilder().setNamespace("economy").setName("coins")
            .setType(ScalarType.LONG).setDefaultValue(Value.newBuilder().setLongValue(0)).build();
    static final Hello.Builder NONE = Hello.newBuilder();

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @TempDir Path dir;
    final List<Client> clients = new ArrayList<>();
    final List<ManagedChannel> channels = new ArrayList<>();
    AgoraServer server;

    @BeforeAll
    static void startPostgres() {
        postgres.start();
    }

    @AfterAll
    static void stopPostgres() {
        postgres.stop();
    }

    AgoraConfig config(int port) throws Exception {
        Path tokens = dir.resolve("tokens");
        Files.writeString(tokens, "# nodes\nproxy-01=tok-p\nlobby-01=tok-l\n");
        return new AgoraConfig(port, postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), 4, tokens,
                Duration.ZERO);
    }

    @AfterEach
    void tearDown() {
        clients.forEach(Client::cancel);
        channels.forEach(ManagedChannel::shutdownNow);
        if (server != null) server.close();
    }

    static Metadata headers(String node) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), node);
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + (node.startsWith("proxy") ? "tok-p" : "tok-l"));
        return md;
    }

    ManagedChannel channel() {
        ManagedChannel ch = NettyChannelBuilder.forAddress("localhost", server.port()).usePlaintext().build();
        channels.add(ch);
        return ch;
    }

    AgoraGrpc.AgoraBlockingStub stub(ManagedChannel ch, String node) {
        return AgoraGrpc.newBlockingStub(ch).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node)))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    Client connect(ManagedChannel ch, String node, NodeKind kind) throws Exception {
        Client c = new Client();
        AgoraGrpc.newStub(ch).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node))).connect(c);
        clients.add(c);
        c.send(NodeMessage.newBuilder().setHello(Hello.newBuilder().setNodeId(node).setKind(kind)).build());
        assertThat(c.nextMatching(ServiceMessage::hasWelcome)).isNotNull();
        return c;
    }

    @Test
    void fullRoundTrip() throws Exception {
        server = AgoraServer.start(config(0));
        ManagedChannel ch = channel();
        Client proxy = connect(ch, "proxy-01", NodeKind.VELOCITY);
        Client lobby = connect(ch, "lobby-01", NodeKind.PAPER);
        String player = UUID.randomUUID().toString();

        var claim = stub(ch, "proxy-01").claimPresence(ClaimPresenceRequest.newBuilder().setEntry(
                PresenceEntry.newBuilder().setPlayer(player).setName("Alex").setProxy("proxy-01")).build());
        assertThat(claim.getAccepted()).isTrue();
        proxy.send(NodeMessage.newBuilder().setPlayerSwitched(
                PlayerSwitched.newBuilder().setPlayer(player).setServer("lobby-01")).build());
        lobby.nextMatching(m -> m.hasPresenceChanged() && m.getPresenceChanged().hasUpsert()
                && m.getPresenceChanged().getUpsert().getServer().equals("lobby-01"));

        var lobbyStub = stub(ch, "lobby-01");
        assertThat(lobbyStub.loadPlayer(LoadPlayerRequest.newBuilder().setPlayer(player).build()).getRowsList())
                .isNotNull();

        var applied = lobbyStub.apply(ApplyRequest.newBuilder().setIdempotencyKey(UUID.randomUUID().toString())
                .setDoc(DocRef.newBuilder().setPlayer(player))
                .addOps(FieldOp.newBuilder().setField(COINS).setInc(50)).build());
        assertThat(applied.getApplied()).isTrue();
        ServiceMessage changed = lobby.nextMatching(ServiceMessage::hasDocChanged);
        assertThat(changed.getDocChanged().getRef().getPlayerRow().getPlayer()).isEqualTo(player);
        assertThat(changed.getDocChanged().getDataCount()).isEqualTo(1);

        lobby.send(NodeMessage.newBuilder().setSendDelivery(SendDelivery.newBuilder().setPlayer(player)
                .setDelivery(Delivery.newBuilder().setKind(DeliveryKind.CHAT).setComponentJson("\"hi\""))).build());
        ServiceMessage deliver = proxy.nextMatching(ServiceMessage::hasDeliver);
        assertThat(deliver.getDeliver().getPlayer()).isEqualTo(player);
        assertThat(deliver.getDeliver().getDelivery().getComponentJson()).isEqualTo("\"hi\"");
    }

    @Test
    void restartOnSamePort() throws Exception {
        server = AgoraServer.start(config(0));
        int port = server.port();
        server.close();
        server = AgoraServer.start(config(port));
        assertThat(server.port()).isEqualTo(port);
        connect(channel(), "lobby-01", NodeKind.PAPER);
    }

    @Test
    void shutdownClosesStreamsWithUnavailable() throws Exception {
        server = AgoraServer.start(config(0));
        ManagedChannel ch = channel();
        Client lobby = connect(ch, "lobby-01", NodeKind.PAPER);
        Client proxy = connect(ch, "proxy-01", NodeKind.VELOCITY);
        server.close();
        assertThat(lobby.closed.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(proxy.closed.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        server = null;
    }

    @Test
    void shutdownHookClosesServerAndIsIdempotentWithClose() throws Exception {
        server = AgoraServer.start(config(0));
        Client lobby = connect(channel(), "lobby-01", NodeKind.PAPER);
        Thread hook = Main.shutdownHook(server);
        assertThat(hook.getName()).isEqualTo("agora-shutdown");
        hook.run();
        assertThat(lobby.closed.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        server.close(); // second close (e.g. the test teardown) must not throw
        server = null;
    }

    static final class Client implements ClientResponseObserver<NodeMessage, ServiceMessage> {
        final LinkedBlockingQueue<ServiceMessage> received = new LinkedBlockingQueue<>();
        final CompletableFuture<Status> closed = new CompletableFuture<>();
        volatile ClientCallStreamObserver<NodeMessage> requests;

        @Override public void beforeStart(ClientCallStreamObserver<NodeMessage> r) { this.requests = r; }
        @Override public void onNext(ServiceMessage value) { received.add(value); }
        @Override public void onError(Throwable t) { closed.complete(Status.fromThrowable(t)); }
        @Override public void onCompleted() { closed.complete(Status.OK); }

        synchronized void send(NodeMessage m) { requests.onNext(m); }

        void cancel() {
            if (requests != null && !closed.isDone()) requests.cancel("test done", null);
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
    }
}

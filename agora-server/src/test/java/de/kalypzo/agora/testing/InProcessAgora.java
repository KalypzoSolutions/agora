package de.kalypzo.agora.testing;

import static org.assertj.core.api.Assertions.assertThat;

import de.kalypzo.agora.auth.AuthInterceptor;
import de.kalypzo.agora.auth.NodeTokens;
import de.kalypzo.agora.delivery.DeliveryRouter;
import de.kalypzo.agora.grpc.NodeStream;
import de.kalypzo.agora.grpc.PresenceEndpoints;
import de.kalypzo.agora.locate.LocateService;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.KickResult;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * In-process Agora without Postgres for the stream-based cards (A-12): node registry, dispatcher, presence, the
 * delivery router and the locate service, behind the real auth interceptor and {@code Connect} stream.
 */
public final class InProcessAgora implements AutoCloseable {

    public static final Map<String, String> TOKENS = Map.of(
            "lobby-01", "tok-l1", "lobby-02", "tok-l2", "proxy-01", "tok-p1", "proxy-02", "tok-p2");

    public final NodeRegistry registry;
    public final NodeMessageDispatcher dispatcher = new NodeMessageDispatcher();
    public final PresenceService presence;
    public final LocateService locate;
    private final ExecutorService lanes = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, CompletableFuture<Void>> markers = new ConcurrentHashMap<>();
    private final List<Client> clients = new ArrayList<>();
    private final Server server;
    public final ManagedChannel channel;

    public InProcessAgora(Path dir, Duration locateTimeout) throws Exception {
        Path tokens = dir.resolve("tokens");
        StringBuilder sb = new StringBuilder();
        TOKENS.forEach((n, t) -> sb.append(n).append('=').append(t).append('\n'));
        Files.writeString(tokens, sb.toString());
        registry = new NodeRegistry(lanes);
        presence = new PresenceService(registry);
        presence.install(dispatcher);
        AgoraService service = new AgoraService();
        PresenceEndpoints.register(service, presence, registry);
        new DeliveryRouter(registry, presence).install(dispatcher);
        locate = new LocateService(registry, presence, locateTimeout);
        locate.install(dispatcher);
        locate.register(service);
        // Registered last: sees a marker only after every earlier message of the node was handled.
        dispatcher.addListener(new NodeMessageListener() {
            @Override
            public void onKickResult(NodeSession session, KickResult result) {
                CompletableFuture<Void> f = markers.remove(result.getRequestId());
                if (f != null) f.complete(null);
            }
        });
        service.onConnect(new NodeStream(registry, dispatcher, presence, Clock.systemUTC(), scheduler,
                Duration.ZERO, NodeSession.DEFAULT_QUEUE_LIMIT));
        String name = "agora-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name)
                .addService(service)
                .intercept(new AuthInterceptor(NodeTokens.load(tokens)))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    public static Metadata headers(String node) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), node);
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + TOKENS.get(node));
        return md;
    }

    public AgoraGrpc.AgoraStub stub(String node) {
        return AgoraGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node)))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    public AgoraGrpc.AgoraBlockingStub blockingStub(String node) {
        return AgoraGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node)))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    /** Connects {@code node} with a Hello ({@code players} for a proxy) and consumes the Welcome. */
    public Client connect(String node, NodeKind kind, PresenceEntry... players) throws Exception {
        Client c = new Client();
        AgoraGrpc.newStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers(node)))
                .connect(c);
        clients.add(c);
        Hello.Builder hello = Hello.newBuilder().setNodeId(node).setKind(kind);
        for (PresenceEntry p : players) hello.addPlayers(p);
        c.send(NodeMessage.newBuilder().setHello(hello).build());
        assertThat(c.nextMatching(ServiceMessage::hasWelcome)).isNotNull();
        return c;
    }

    /** Waits until the server has handled every message {@code c} sent so far (marker on the same lane). */
    public void processed(Client c) throws Exception {
        String id = "marker-" + System.nanoTime();
        CompletableFuture<Void> seen = new CompletableFuture<>();
        markers.put(id, seen);
        c.send(NodeMessage.newBuilder().setKickResult(KickResult.newBuilder().setRequestId(id)).build());
        seen.get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        clients.forEach(Client::cancel);
        channel.shutdownNow();
        server.shutdownNow();
        registry.close(Duration.ofSeconds(1));
        scheduler.shutdownNow();
        lanes.shutdownNow();
    }

    public static final class Client implements ClientResponseObserver<NodeMessage, ServiceMessage> {
        public final LinkedBlockingQueue<ServiceMessage> received = new LinkedBlockingQueue<>();
        final CompletableFuture<Status> closed = new CompletableFuture<>();
        volatile ClientCallStreamObserver<NodeMessage> requests;

        @Override public void beforeStart(ClientCallStreamObserver<NodeMessage> r) { this.requests = r; }
        @Override public void onNext(ServiceMessage value) { received.add(value); }
        @Override public void onError(Throwable t) { closed.complete(Status.fromThrowable(t)); }
        @Override public void onCompleted() { closed.complete(Status.OK); }

        public synchronized void send(NodeMessage m) {
            requests.onNext(m);
        }

        public void cancel() {
            if (requests != null && !closed.isDone()) requests.cancel("test done", null);
        }

        /** Skips messages until one matches, at most 5 s. */
        public ServiceMessage nextMatching(Predicate<ServiceMessage> p) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < until) {
                ServiceMessage m = received.poll(until - System.nanoTime(), TimeUnit.NANOSECONDS);
                if (m == null) break;
                if (p.test(m)) return m;
            }
            throw new AssertionError("no matching message within 5 s");
        }

        /** Every message received so far that matches (non-blocking). */
        public List<ServiceMessage> drained(Predicate<ServiceMessage> p) {
            List<ServiceMessage> all = new ArrayList<>();
            received.drainTo(all);
            return all.stream().filter(p).toList();
        }
    }
}

package de.kalypzo.agora.grpc;

import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceSnapshot;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.Welcome;
import de.kalypzo.agora.service.ConnectHandler;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code Connect} stream (agora §3): Hello/Welcome handshake, session registration and dispatch of every further
 * {@code NodeMessage} to the {@link NodeMessageDispatcher}. Register with {@code AgoraService.onConnect(stream)}.
 *
 * <p>Rules: the first message must be {@code Hello} ({@code INVALID_ARGUMENT} otherwise) and its {@code node_id}
 * must be the authenticated node ({@code PERMISSION_DENIED}). After {@code Hello} the session is registered, the
 * hello listeners run, and {@code Welcome} with the presence snapshot is sent: to VELOCITY nodes at once, to PAPER
 * nodes no earlier than {@code startedAt + paperWelcomeDelay} (E11, R1). Until then all other messages flow normally.
 *
 * <p>Pending PAPER welcomes are released by {@link #tick()}, which runs on the injected scheduler when the warm-up
 * ends and can be called directly by tests with a fake {@link Clock} (R24).
 */
public final class NodeStream implements ConnectHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStream.class);

    private final NodeRegistry registry;
    private final NodeMessageDispatcher dispatcher;
    private final PresenceSnapshot presence;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final int queueLimit;
    private final Instant welcomePaperAt;
    private final ConcurrentHashMap<NodeSession, Boolean> pendingWelcomes = new ConcurrentHashMap<>();

    public NodeStream(
            NodeRegistry registry,
            NodeMessageDispatcher dispatcher,
            PresenceSnapshot presence,
            Clock clock,
            ScheduledExecutorService scheduler,
            Duration paperWelcomeDelay,
            int queueLimit) {
        this.registry = registry;
        this.dispatcher = dispatcher;
        this.presence = presence;
        this.clock = clock;
        this.scheduler = scheduler;
        this.queueLimit = queueLimit;
        this.welcomePaperAt = clock.instant().plus(paperWelcomeDelay);
    }

    @Override
    public StreamObserver<NodeMessage> connect(StreamObserver<ServiceMessage> out) {
        NodeIdentity identity = NodeIdentity.CONTEXT_KEY.get(); // capture before any async work (A-03)
        AtomicReference<NodeSession> session = new AtomicReference<>();
        if (out instanceof ServerCallStreamObserver<ServiceMessage> call) {
            call.setOnReadyHandler(() -> {
                NodeSession s = session.get();
                if (s != null) s.drain();
            });
            call.setOnCancelHandler(() -> {
                NodeSession s = session.get();
                if (s != null) s.peerClosed(Status.CANCELLED.withDescription("cancelled by node"));
            });
        }
        return new StreamObserver<>() {
            private boolean failed; // only touched on the (serialized) inbound callback thread

            @Override
            public void onNext(NodeMessage m) {
                if (failed) return;
                NodeSession s = session.get();
                if (s == null) {
                    s = handshake(identity, m, out);
                    if (s == null) {
                        failed = true;
                        return;
                    }
                    session.set(s);
                    registry.register(s);
                    dispatcher.dispatch(s, m);
                    welcome(s);
                    return;
                }
                if (m.hasHello()) {
                    s.close(Status.INVALID_ARGUMENT.withDescription("Hello was already sent on this stream"));
                    return;
                }
                if (!s.isOpen()) return;
                dispatcher.dispatch(s, m);
            }

            @Override
            public void onError(Throwable t) {
                NodeSession s = session.get();
                if (s != null) s.peerClosed(Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {
                NodeSession s = session.get();
                if (s != null) s.close(Status.OK);
                else if (!failed) out.onCompleted();
            }
        };
    }

    /** Validates the first message; returns the new session, or {@code null} after closing the stream. */
    private NodeSession handshake(NodeIdentity identity, NodeMessage m, StreamObserver<ServiceMessage> out) {
        Status reject = null;
        if (identity == null) {
            reject = Status.UNAUTHENTICATED.withDescription("no authenticated node");
        } else if (!m.hasHello()) {
            reject = Status.INVALID_ARGUMENT.withDescription("first message must be Hello, got " + m.getPayloadCase());
        } else if (!m.getHello().getNodeId().equals(identity.nodeId())) {
            LOG.warn("Node '{}' sent Hello as '{}'", identity.nodeId(), m.getHello().getNodeId());
            reject = Status.PERMISSION_DENIED.withDescription("hello.node_id does not match the authenticated node");
        } else if (m.getHello().getKind() != NodeKind.PAPER && m.getHello().getKind() != NodeKind.VELOCITY) {
            reject = Status.INVALID_ARGUMENT.withDescription("hello.kind must be PAPER or VELOCITY");
        } else if (registry.isShuttingDown()) {
            reject = Status.UNAVAILABLE.withDescription("shutting down");
        }
        if (reject != null) {
            out.onError(reject.asRuntimeException());
            return null;
        }
        Hello hello = m.getHello();
        LOG.info("Node '{}' ({}) connected", hello.getNodeId(), hello.getKind());
        return new NodeSession(hello.getNodeId(), hello.getKind(), clock.instant(), out, queueLimit);
    }

    private void welcome(NodeSession s) {
        if (s.kind() == NodeKind.PAPER) {
            Instant now = clock.instant();
            if (now.isBefore(welcomePaperAt)) {
                pendingWelcomes.put(s, Boolean.TRUE);
                scheduleTick(Duration.between(now, welcomePaperAt));
                LOG.debug("Welcome for '{}' waits for the warm-up until {}", s.nodeId(), welcomePaperAt);
                return;
            }
        }
        sendWelcome(s);
    }

    /** Sends the pending PAPER welcomes once the warm-up is over (R24 test hook). */
    public void tick() {
        if (pendingWelcomes.isEmpty()) return;
        Instant now = clock.instant();
        if (now.isBefore(welcomePaperAt)) {
            scheduleTick(Duration.between(now, welcomePaperAt));
            return;
        }
        for (NodeSession s : pendingWelcomes.keySet()) {
            if (pendingWelcomes.remove(s) != null && s.isOpen()) sendWelcome(s);
        }
    }

    private void scheduleTick(Duration delay) {
        try {
            scheduler.schedule(this::tick, Math.max(1, delay.toMillis()), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            LOG.debug("Scheduler stopped, welcome tick not scheduled");
        }
    }

    private void sendWelcome(NodeSession s) {
        s.send(ServiceMessage.newBuilder()
                .setWelcome(Welcome.newBuilder().addAllPresence(presence.snapshot()))
                .build());
    }
}

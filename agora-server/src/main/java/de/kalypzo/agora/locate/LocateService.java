package de.kalypzo.agora.locate;

import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.node.Subscription;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.proto.v1.LocateResponse;
import de.kalypzo.agora.proto.v1.LocationReply;
import de.kalypzo.agora.proto.v1.LocationRequest;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Locate} (E1): the presence entry of the player names its server; the session with {@code nodeId ==
 * entry.server} gets a {@code LocationRequest(request_id)}, and the call waits for the matching
 * {@code LocationReply}, at most the timeout (default {@value #DEFAULT_TIMEOUT_SECONDS} s).
 *
 * <ul>
 *   <li>Player offline, not on a server yet ({@code server == ""}), or reply {@code found = false}: {@code found =
 *       false} (Pantheon maps it to the not-online error, PresenceMessagingContract).
 *   <li>No open session of the server node, timeout, or the server's session ends or is replaced before it answers:
 *       {@code UNAVAILABLE} (R42).
 * </ul>
 *
 * The wait is a future (never on a node lane or the RPC thread); a reply is accepted only from the session that was
 * asked, and only while that session is not superseded. Register with {@link #install} and {@link #register}.
 */
public final class LocateService implements NodeMessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(LocateService.class);

    static final int DEFAULT_TIMEOUT_SECONDS = 3;
    /** E1: wait at most 3 s for the server of the player. */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS);

    private record Pending(NodeSession server, CompletableFuture<LocationReply> reply) {}

    private final NodeRegistry nodes;
    private final PresenceService presence;
    private final Duration timeout;
    private final ConcurrentHashMap<String, Pending> pending = new ConcurrentHashMap<>();

    public LocateService(NodeRegistry nodes, PresenceService presence, Duration timeout) {
        this.nodes = nodes;
        this.presence = presence;
        this.timeout = timeout;
    }

    public LocateService(NodeRegistry nodes, PresenceService presence) {
        this(nodes, presence, DEFAULT_TIMEOUT);
    }

    /** Subscribes to {@code LocationReply} and to node-gone (pending requests of a gone server end at once). */
    public Subscription install(NodeMessageDispatcher dispatcher) {
        Subscription replies = dispatcher.addListener(this);
        Subscription gone = nodes.onNodeGone(this::nodeGone);
        return () -> {
            replies.cancel();
            gone.cancel();
        };
    }

    /**
     * Registers {@code Locate} on {@code service} (R5). An invalid UUID is {@code INVALID_ARGUMENT}, during shutdown
     * {@code UNAVAILABLE}. The handler does not block.
     */
    public void register(AgoraService service) {
        service.onLocate((req, out) -> {
            UUID player;
            try {
                player = UUID.fromString(req.getPlayer());
            } catch (IllegalArgumentException e) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("player is not a UUID").asRuntimeException());
                return;
            }
            locate(player).whenComplete((response, error) -> {
                if (error == null) {
                    out.onNext(response);
                    out.onCompleted();
                    return;
                }
                Throwable cause = error instanceof CompletionException && error.getCause() != null
                        ? error.getCause() : error;
                if (cause instanceof StatusException se) {
                    out.onError(se.getStatus().asRuntimeException());
                } else {
                    LOG.error("Locate of {} failed", player, cause);
                    out.onError(Status.INTERNAL.withDescription("internal error").asRuntimeException());
                }
            });
        });
    }

    /**
     * Locates {@code player}. Completes with the answer, or exceptionally with a {@link StatusException}
     * ({@code UNAVAILABLE}).
     */
    public CompletableFuture<LocateResponse> locate(UUID player) {
        if (nodes.isShuttingDown()) return unavailable("shutting down");
        Optional<PresenceEntry> entry = presence.entry(player);
        if (entry.isEmpty() || entry.get().getServer().isEmpty()) return CompletableFuture.completedFuture(notFound());
        String serverId = entry.get().getServer();
        NodeSession server = nodes.get(serverId).filter(NodeSession::isOpen).orElse(null);
        if (server == null) return unavailable("server '" + serverId + "' of the player is not connected");

        String requestId = UUID.randomUUID().toString();
        CompletableFuture<LocationReply> reply = new CompletableFuture<>();
        pending.put(requestId, new Pending(server, reply));
        // nodeGone may have run before the put above: re-check after registering.
        boolean sent = server.isOpen() && server.send(ServiceMessage.newBuilder()
                .setLocationRequest(LocationRequest.newBuilder().setRequestId(requestId).setPlayer(player.toString()))
                .build());
        if (!sent) reply.complete(null);
        // Async continuation: a timeout completes on the JDK-wide delayer thread, which must not do the work.
        return reply.completeOnTimeout(null, timeout.toMillis(), TimeUnit.MILLISECONDS)
                .handleAsync((r, e) -> {
                    pending.remove(requestId);
                    // Re-check after the wait: an answer counts only from the asked, not superseded session.
                    if (r == null || e != null || nodes.isSuperseded(server)) {
                        LOG.debug("Locate of {}: server '{}' did not answer within {}", player, serverId, timeout);
                        throw new CompletionException(Status.UNAVAILABLE
                                .withDescription("server '" + serverId + "' did not answer").asException());
                    }
                    if (!r.getFound()) return notFound();
                    return LocateResponse.newBuilder().setFound(true).setLocation(r.getLocation()).build();
                });
    }

    @Override
    public void onLocationReply(NodeSession session, LocationReply reply) {
        Pending p = pending.get(reply.getRequestId());
        if (p == null || p.server() != session || nodes.isSuperseded(session)) {
            LOG.debug("LocationReply '{}' from '{}' matches no pending locate", reply.getRequestId(), session.nodeId());
            return;
        }
        p.reply().complete(reply);
    }

    private void nodeGone(NodeSession session) {
        for (Pending p : pending.values()) {
            if (p.server() == session) p.reply().complete(null); // it will not answer
        }
    }

    private static LocateResponse notFound() {
        return LocateResponse.newBuilder().setFound(false).build();
    }

    private static <T> CompletableFuture<T> unavailable(String why) {
        return CompletableFuture.failedFuture(Status.UNAVAILABLE.withDescription(why).asException());
    }
}

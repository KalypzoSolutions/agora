package de.kalypzo.agora.teleport;

import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.node.Subscription;
import de.kalypzo.agora.presence.PresenceEvents;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.presence.RemovalReason;
import de.kalypzo.agora.proto.v1.ArrivalFailed;
import de.kalypzo.agora.proto.v1.ArrivalPrepared;
import de.kalypzo.agora.proto.v1.CancelArrival;
import de.kalypzo.agora.proto.v1.ConnectPlayer;
import de.kalypzo.agora.proto.v1.ConnectResult;
import de.kalypzo.agora.proto.v1.NetworkLocation;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.PlayerArrived;
import de.kalypzo.agora.proto.v1.PrepareArrival;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.TeleportResponse;
import de.kalypzo.agora.proto.v1.TeleportStatus;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Teleport}: moves a player to a PAPER node, optionally to a location there. Agora sends
 * {@code PrepareArrival} to the target, on {@code READY} {@code ConnectPlayer} to the player's proxy, and answers once
 * the target reports {@code PlayerArrived} (or a step fails or times out, see {@link AgoraConfig.TeleportTimeouts}).
 *
 * <p>State per player (a slot): the running teleport ({@code current}) and the last {@code ConnectPlayer} that has no
 * {@code ConnectResult} yet ({@code openConnect}). Every transition of a player runs under the lock of its slot and
 * only changes state; sends and the completion of result futures are collected and run after the lock is released.
 * A new teleport supersedes the running one and starts only once no {@code ConnectPlayer} of the player is open
 * (until then it waits). Answers are accepted only from the session that was asked, while it is not superseded. A
 * gone target ends the teleport with {@code SERVER_UNAVAILABLE}, a gone proxy or a removed presence entry with
 * {@code NOT_ONLINE}. Timers run on the common pool and do nothing once their flow has ended or moved to another
 * phase. Register with {@link #install} (before the server starts) and {@link #register}.
 */
public final class TeleportService implements NodeMessageListener, PresenceEvents {

    private static final Logger LOG = LoggerFactory.getLogger(TeleportService.class);

    private enum Phase { WAITING, PREPARING, CONNECTING, ARRIVING }

    private static final class Flow {
        final String requestId = UUID.randomUUID().toString();
        final UUID player;
        final String server;
        final NetworkLocation location; // null: connect only
        final CompletableFuture<TeleportStatus> result = new CompletableFuture<>();
        Phase phase = Phase.WAITING;
        NodeSession target;
        NodeSession proxy;
        boolean prepareSent;
        boolean connectAnswered;
        volatile boolean done; // also read by the PrepareArrival send, outside the lock

        Flow(UUID player, String server, NetworkLocation location) {
            this.player = player;
            this.server = server;
            this.location = location;
        }
    }

    private record OpenConnect(String requestId, NodeSession proxy) {}

    private static final class Slot {
        Flow current;
        OpenConnect openConnect;
        boolean removed;
    }

    private final NodeRegistry nodes;
    private final PresenceService presence;
    private final AgoraConfig.TeleportTimeouts timeouts;
    private final ConcurrentHashMap<UUID, Slot> slots = new ConcurrentHashMap<>();
    /** Flows that sent a PrepareArrival, until they ended and their ConnectPlayer (if any) is no longer open. */
    private final ConcurrentHashMap<String, Flow> flows = new ConcurrentHashMap<>();

    public TeleportService(NodeRegistry nodes, PresenceService presence, AgoraConfig.TeleportTimeouts timeouts) {
        this.nodes = nodes;
        this.presence = presence;
        this.timeouts = timeouts;
    }

    /**
     * Subscribes to the teleport answers of the nodes, to node-gone and to the presence events. Call before the server
     * starts.
     */
    public Subscription install(NodeMessageDispatcher dispatcher) {
        presence.addEvents(this);
        Subscription answers = dispatcher.addListener(this);
        Subscription gone = nodes.onNodeGone(this::nodeGone);
        return () -> {
            answers.cancel();
            gone.cancel();
        };
    }

    /**
     * Registers {@code Teleport} on {@code service}. Only an open PAPER session may call it: without identity
     * {@code UNAUTHENTICATED}, during shutdown {@code UNAVAILABLE}, without open session {@code FAILED_PRECONDITION},
     * from another node kind {@code PERMISSION_DENIED}. An invalid UUID, an empty {@code server} or a
     * {@code location.server} other than {@code server} is {@code INVALID_ARGUMENT}. Otherwise the answer is always a
     * {@link TeleportResponse}. The handler does not block.
     */
    public void register(AgoraService service) {
        service.onTeleport((req, out) -> {
            NodeIdentity caller = NodeIdentity.CONTEXT_KEY.get();
            if (caller == null) {
                out.onError(Status.UNAUTHENTICATED.withDescription("no node identity").asRuntimeException());
                return;
            }
            if (nodes.isShuttingDown()) {
                out.onError(Status.UNAVAILABLE.withDescription("shutting down").asRuntimeException());
                return;
            }
            NodeSession session = nodes.get(caller.nodeId()).filter(NodeSession::isOpen).orElse(null);
            if (session == null) {
                out.onError(Status.FAILED_PRECONDITION.withDescription("no open Connect stream for node '"
                        + caller.nodeId() + "'").asRuntimeException());
                return;
            }
            if (session.kind() != NodeKind.PAPER) {
                out.onError(Status.PERMISSION_DENIED.withDescription("only PAPER nodes teleport players")
                        .asRuntimeException());
                return;
            }
            UUID player;
            try {
                player = UUID.fromString(req.getPlayer());
            } catch (IllegalArgumentException e) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("player is not a UUID").asRuntimeException());
                return;
            }
            if (req.getServer().isEmpty()) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("server is required").asRuntimeException());
                return;
            }
            if (req.hasLocation() && !req.getLocation().getServer().equals(req.getServer())) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("location.server differs from server")
                        .asRuntimeException());
                return;
            }
            teleport(player, req.getServer(), req.hasLocation() ? req.getLocation() : null).thenAccept(status -> {
                out.onNext(TeleportResponse.newBuilder().setStatus(status).build());
                out.onCompleted();
            });
        });
    }

    /**
     * Teleports {@code player} to {@code server}, to {@code location} there, or only connects it when
     * {@code location} is {@code null}. A running teleport of the player ends with {@code SUPERSEDED}; while a
     * {@code ConnectPlayer} of the player is open, this one waits for it. The future always completes normally.
     */
    public CompletableFuture<TeleportStatus> teleport(UUID player, String server, NetworkLocation location) {
        Objects.requireNonNull(player, "player");
        Objects.requireNonNull(server, "server");
        Flow flow = new Flow(player, server, location);
        transition(player, true, (slot, actions) -> {
            if (slot.current != null) finish(slot, slot.current, TeleportStatus.SUPERSEDED, actions);
            slot.current = flow;
            after(timeouts.total(), player, (s, a) -> {
                if (!flow.done) finish(s, flow, TeleportStatus.TIMEOUT, a);
            });
            if (slot.openConnect == null) start(slot, flow, actions); // else WAITING until it is cleared (AR-8)
        });
        return flow.result;
    }

    @Override
    public void onArrivalPrepared(NodeSession session, ArrivalPrepared prepared) {
        onAnswer(session, prepared.getRequestId(), "ArrivalPrepared", EnumSet.of(Phase.PREPARING),
                (slot, flow, actions) -> {
                    switch (prepared.getStatus()) {
                        case READY -> {
                            flow.phase = Phase.CONNECTING; // AR-2: no other ConnectPlayer of the player is open here
                            OpenConnect open = new OpenConnect(flow.requestId, flow.proxy);
                            slot.openConnect = open;
                            NodeSession proxy = flow.proxy;
                            ServiceMessage connect = ServiceMessage.newBuilder().setConnectPlayer(ConnectPlayer
                                    .newBuilder().setRequestId(flow.requestId).setPlayer(flow.player.toString())
                                    .setServer(flow.server)).build();
                            actions.add(() -> proxy.send(connect));
                            // The open ConnectPlayer expires as long after its sending as the ConnectResult may take.
                            after(timeouts.connect(), flow.player, (s, a) -> {
                                if (s.openConnect == open) clearOpenConnect(s, a);
                            });
                            after(timeouts.connect(), flow.player, (s, a) -> {
                                if (!flow.done && flow.phase == Phase.CONNECTING && !flow.connectAnswered) {
                                    finish(s, flow, TeleportStatus.TIMEOUT, a);
                                }
                            });
                            arrivalTimer(flow);
                        }
                        case ALREADY_HERE -> {
                            flow.phase = Phase.ARRIVING;
                            arrivalTimer(flow);
                        }
                        case ARRIVAL_WORLD_NOT_FOUND -> finish(slot, flow, TeleportStatus.WORLD_NOT_FOUND, actions);
                        default -> finish(slot, flow, TeleportStatus.SERVER_UNAVAILABLE, actions);
                    }
                });
    }

    @Override
    public void onPlayerArrived(NodeSession session, PlayerArrived arrived) {
        onAnswer(session, arrived.getRequestId(), "PlayerArrived",
                EnumSet.of(Phase.PREPARING, Phase.CONNECTING, Phase.ARRIVING),
                (slot, flow, actions) -> finish(slot, flow, TeleportStatus.OK, actions));
    }

    @Override
    public void onArrivalFailed(NodeSession session, ArrivalFailed failed) {
        onAnswer(session, failed.getRequestId(), "ArrivalFailed",
                EnumSet.of(Phase.PREPARING, Phase.CONNECTING, Phase.ARRIVING),
                (slot, flow, actions) -> finish(slot, flow, TeleportStatus.CONNECT_FAILED, actions));
    }

    @Override
    public void onConnectResult(NodeSession session, ConnectResult result) {
        String id = result.getRequestId();
        Flow flow = flows.get(id);
        if (flow == null || nodes.isSuperseded(session)) {
            LOG.debug("ConnectResult '{}' from '{}' matches no teleport", id, session.nodeId());
            return;
        }
        transition(flow.player, false, (slot, actions) -> {
            // Always clears the open ConnectPlayer, even of an ended teleport.
            if (slot.openConnect != null && slot.openConnect.requestId().equals(id)
                    && slot.openConnect.proxy() == session) {
                clearOpenConnect(slot, actions);
            }
            if (flow.done || flow.proxy != session || flow.phase != Phase.CONNECTING || flow.connectAnswered) {
                LOG.debug("ConnectResult '{}' from '{}' is late or unexpected", id, session.nodeId());
                return;
            }
            switch (result.getStatus()) {
                case CONNECTED, ALREADY_CONNECTED -> flow.connectAnswered = true;
                case PLAYER_NOT_FOUND -> finish(slot, flow, TeleportStatus.NOT_ONLINE, actions);
                default -> finish(slot, flow, TeleportStatus.CONNECT_FAILED, actions);
            }
        });
    }

    /** AR-11: the player left the network (or the entry was displaced); its teleport cannot reach it. */
    @Override
    public void removed(PresenceEntry entry, RemovalReason reason) {
        transition(UUID.fromString(entry.getPlayer()), false, (slot, actions) -> {
            if (slot.current != null) finish(slot, slot.current, TeleportStatus.NOT_ONLINE, actions);
        });
    }

    /** AR-10: a gone target ends its teleport (no cancel, it is closed); a gone proxy takes its player with it. */
    private void nodeGone(NodeSession session) {
        for (UUID player : slots.keySet()) {
            transition(player, false, (slot, actions) -> {
                Flow flow = slot.current;
                if (flow != null && flow.target == session) {
                    finish(slot, flow, TeleportStatus.SERVER_UNAVAILABLE, actions);
                } else if (flow != null && flow.proxy == session) {
                    finish(slot, flow, TeleportStatus.NOT_ONLINE, actions);
                }
                if (slot.openConnect != null && slot.openConnect.proxy() == session) clearOpenConnect(slot, actions);
            });
        }
    }

    private interface Answer {
        void apply(Slot slot, Flow flow, List<Runnable> actions);
    }

    /** An answer of the target node: only from the flow's target session, while running and in one of {@code phases}. */
    private void onAnswer(NodeSession session, String id, String what, EnumSet<Phase> phases, Answer answer) {
        Flow flow = flows.get(id);
        if (flow == null || flow.target != session || nodes.isSuperseded(session)) {
            LOG.debug("{} '{}' from '{}' matches no teleport", what, id, session.nodeId());
            return;
        }
        transition(flow.player, false, (slot, actions) -> {
            if (flow.done || !phases.contains(flow.phase)) {
                LOG.debug("{} '{}' from '{}' is late or unexpected", what, id, session.nodeId());
                return;
            }
            answer.apply(slot, flow, actions);
        });
    }

    /** AR-3: checks presence, target and proxy, then sends PrepareArrival. */
    private void start(Slot slot, Flow flow, List<Runnable> actions) {
        PresenceEntry entry = presence.entry(flow.player).orElse(null);
        if (entry == null || entry.getServer().isEmpty()) {
            finish(slot, flow, TeleportStatus.NOT_ONLINE, actions);
            return;
        }
        NodeSession target = nodes.get(flow.server).filter(NodeSession::isOpen)
                .filter(s -> s.kind() == NodeKind.PAPER).orElse(null);
        NodeSession proxy = nodes.get(entry.getProxy()).filter(NodeSession::isOpen).orElse(null);
        if (target == null || proxy == null) {
            LOG.debug("Teleport of {} to '{}': target or proxy '{}' not connected", flow.player, flow.server,
                    entry.getProxy());
            finish(slot, flow, TeleportStatus.SERVER_UNAVAILABLE, actions);
            return;
        }
        flow.target = target;
        flow.proxy = proxy;
        flow.prepareSent = true;
        flow.phase = Phase.PREPARING;
        flows.put(flow.requestId, flow);
        PrepareArrival.Builder prepare = PrepareArrival.newBuilder()
                .setRequestId(flow.requestId).setPlayer(flow.player.toString());
        if (flow.location != null) prepare.setLocation(flow.location);
        ServiceMessage message = ServiceMessage.newBuilder().setPrepareArrival(prepare).build();
        actions.add(() -> {
            // ended in between (e.g. superseded from another action): its Cancel may already be out, so no Prepare
            if (!flow.done) target.send(message);
        });
        after(timeouts.prepare(), flow.player, (s, a) -> {
            if (!flow.done && flow.phase == Phase.PREPARING) finish(s, flow, TeleportStatus.TIMEOUT, a);
        });
    }

    private void arrivalTimer(Flow flow) {
        after(timeouts.arrival(), flow.player, (s, a) -> {
            if (!flow.done && (flow.phase == Phase.CONNECTING || flow.phase == Phase.ARRIVING)) {
                finish(s, flow, TeleportStatus.TIMEOUT, a);
            }
        });
    }

    /** AR-9: ends {@code flow} exactly once; a non-OK end after PrepareArrival cancels the arrival on the target. */
    private void finish(Slot slot, Flow flow, TeleportStatus status, List<Runnable> actions) {
        if (flow.done) return;
        flow.done = true;
        if (slot.current == flow) slot.current = null;
        if (slot.openConnect == null || !slot.openConnect.requestId().equals(flow.requestId)) {
            flows.remove(flow.requestId);
        }
        LOG.debug("Teleport '{}' of {} to '{}': {}", flow.requestId, flow.player, flow.server, status);
        NodeSession target = flow.target;
        ServiceMessage cancel = flow.prepareSent && status != TeleportStatus.OK
                ? ServiceMessage.newBuilder().setCancelArrival(CancelArrival.newBuilder()
                        .setRequestId(flow.requestId).setPlayer(flow.player.toString())).build()
                : null;
        actions.add(() -> {
            if (cancel != null && target.isOpen()) target.send(cancel);
            flow.result.complete(status);
        });
    }

    /** AR-8: clears the open ConnectPlayer; a waiting teleport starts now. */
    private void clearOpenConnect(Slot slot, List<Runnable> actions) {
        String requestId = slot.openConnect.requestId();
        slot.openConnect = null;
        Flow flow = flows.get(requestId);
        if (flow != null && flow.done) flows.remove(requestId);
        if (slot.current != null && slot.current.phase == Phase.WAITING) start(slot, slot.current, actions);
    }

    /** Runs {@code step} on the player's slot after {@code delay}, on the common pool. */
    private void after(Duration delay, UUID player, BiConsumer<Slot, List<Runnable>> step) {
        CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS)
                .execute(() -> transition(player, false, step));
    }

    /**
     * Runs {@code step} under the lock of the player's slot, then the collected actions outside of it. Without
     * {@code create} a missing slot means there is nothing to do; an emptied slot is removed.
     */
    private void transition(UUID player, boolean create, BiConsumer<Slot, List<Runnable>> step) {
        List<Runnable> actions = new ArrayList<>();
        while (true) {
            Slot slot = create ? slots.computeIfAbsent(player, k -> new Slot()) : slots.get(player);
            if (slot == null) return;
            synchronized (slot) {
                if (slot.removed) {
                    if (create) continue;
                    return;
                }
                step.accept(slot, actions);
                if (slot.current == null && slot.openConnect == null) {
                    slots.remove(player, slot);
                    slot.removed = true;
                }
            }
            break;
        }
        for (Runnable action : actions) {
            try {
                action.run();
            } catch (RuntimeException e) {
                LOG.error("Teleport action for {} failed", player, e); // the other actions (e.g. a complete) still run
            }
        }
    }
}

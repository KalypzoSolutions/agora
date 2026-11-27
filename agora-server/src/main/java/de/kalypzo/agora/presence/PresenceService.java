package de.kalypzo.agora.presence;

import de.kalypzo.agora.node.HelloListener;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.node.Subscription;
import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.KickPlayer;
import de.kalypzo.agora.proto.v1.KickResult;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.PlayerLeft;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.PresenceChanged;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The network presence (agora §4), fed by the VELOCITY proxies and copied to every node.
 *
 * <ul>
 *   <li>{@link #claim}: login over a proxy. Serialized per UUID (a future chain per player). An entry of another
 *       proxy is resolved by {@code KickPlayer} to that proxy and waiting (asynchronously, at most the kick timeout,
 *       default {@value #DEFAULT_KICK_TIMEOUT_SECONDS} s) for its {@code KickResult}; without an open session there is
 *       no wait. The old entry is removed in any case and the new one created ({@link RemovalReason#REPLACED}).
 *   <li>{@code PlayerSwitched}/{@code PlayerLeft}: only from the owning proxy, otherwise ignored. Both run in the
 *       player's claim chain (R23), so a {@code PlayerLeft} of the old proxy during a double login finds the entry
 *       already replaced and is ignored: the removal counts as REPLACED, not LEFT.
 *   <li>Proxy {@code Hello}/{@code Heartbeat}: reconciliation. Missing entries are added, the server is updated,
 *       entries of this proxy that are not listed are removed unless younger than {@value #GRACE_SECONDS} s (E11).
 *       Players with a running claim chain are skipped. No {@link PresenceEvents} for reconciliation.
 *   <li>Node-gone of a VELOCITY session that is not superseded (R2): all its players are removed
 *       ({@link RemovalReason#PROXY_LOST}). After shutdown (NOT_SERVING) nothing changes any more.
 * </ul>
 *
 * Every change is broadcast as {@code PresenceChanged} to all sessions, under the presence lock, so broadcasts and
 * {@link #snapshot()} (the {@code Welcome}) are totally ordered. Lane handlers never block: the kick wait is a
 * future. Register with {@link #install}.
 */
public final class PresenceService implements PresenceSnapshot, HelloListener, NodeMessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(PresenceService.class);

    static final int DEFAULT_KICK_TIMEOUT_SECONDS = 3;
    static final int GRACE_SECONDS = 10;
    /** Globals: wait at most 3 s for the kick answer in a double login. */
    public static final Duration DEFAULT_KICK_TIMEOUT = Duration.ofSeconds(DEFAULT_KICK_TIMEOUT_SECONDS);
    /** E11: reconciliation never removes entries younger than this. */
    public static final Duration RECONCILE_GRACE = Duration.ofSeconds(GRACE_SECONDS);

    private record Slot(PresenceEntry entry, Instant createdAt) {}

    private record PendingKick(String nodeId, CompletableFuture<Boolean> answer) {}

    private final NodeRegistry registry;
    private final Clock clock;
    private final Duration kickTimeout;
    private final PresenceEvents events;
    private final Object lock = new Object();
    private final Map<UUID, Slot> entries = new LinkedHashMap<>(); // guarded by lock
    private final ConcurrentHashMap<UUID, CompletableFuture<?>> chains = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PendingKick> kicks = new ConcurrentHashMap<>();

    /**
     * @param clock timestamps ({@code online_since}, reconcile grace; R24)
     * @param kickTimeout longest wait for the old proxy's {@code KickResult}
     * @param events real presence events (A-19); never for reconciliation
     */
    public PresenceService(NodeRegistry registry, Clock clock, Duration kickTimeout, PresenceEvents events) {
        this.registry = registry;
        this.clock = clock;
        this.kickTimeout = kickTimeout;
        this.events = events;
    }

    public PresenceService(NodeRegistry registry) {
        this(registry, Clock.systemUTC(), DEFAULT_KICK_TIMEOUT, PresenceEvents.NOOP);
    }

    /** Subscribes to the node messages (Hello, Heartbeat, KickResult, PlayerSwitched, PlayerLeft) and node-gone. */
    public Subscription install(NodeMessageDispatcher dispatcher) {
        Subscription messages = dispatcher.addListener(this);
        Subscription gone = registry.onNodeGone(this::nodeGone);
        return () -> {
            messages.cancel();
            gone.cancel();
        };
    }

    // ---------- claim ----------

    /**
     * Login of {@code entry.player} over {@code proxyNodeId}. {@code proxy} is set to the caller, {@code online_since}
     * to now, {@code server} to "". Completes with {@code true} once the entry exists; with {@code false} if the
     * claiming proxy has no open session any more after the kick wait (its login cannot complete); exceptionally
     * with {@link IllegalStateException} if the proxy has no open session, {@link IllegalArgumentException} for an
     * invalid UUID.
     */
    public CompletableFuture<Boolean> claim(String proxyNodeId, PresenceEntry entry) {
        UUID player = UUID.fromString(entry.getPlayer());
        return chain(player, () -> doClaim(proxyNodeId, player, entry));
    }

    private CompletableFuture<Boolean> doClaim(String proxy, UUID player, PresenceEntry entry) {
        if (openSession(proxy).isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException("node '" + proxy + "' has no open stream"));
        }
        PresenceEntry fresh = entry.toBuilder()
                .setProxy(proxy)
                .setServer("")
                .setOnlineSinceMillis(clock.millis())
                .build();
        String oldProxy;
        synchronized (lock) {
            Slot slot = entries.get(player);
            oldProxy = slot == null ? null : slot.entry().getProxy();
        }
        if (oldProxy == null || oldProxy.equals(proxy)) {
            return CompletableFuture.completedFuture(put(proxy, player, fresh));
        }
        return kick(oldProxy, player).thenApply(v -> put(proxy, player, fresh));
    }

    /** Sends {@code KickPlayer} to {@code oldProxy} and completes after its answer, the timeout, or at once. */
    private CompletableFuture<Void> kick(String oldProxy, UUID player) {
        Optional<NodeSession> target = openSession(oldProxy);
        if (target.isEmpty()) {
            LOG.info("Double login of {}: old proxy '{}' has no stream, replacing at once", player, oldProxy);
            return CompletableFuture.completedFuture(null);
        }
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<Boolean> answer = new CompletableFuture<>();
        kicks.put(requestId, new PendingKick(oldProxy, answer));
        boolean sent = target.get().send(ServiceMessage.newBuilder()
                .setKickPlayer(KickPlayer.newBuilder().setRequestId(requestId).setPlayer(player.toString()))
                .build());
        if (!sent) answer.complete(Boolean.FALSE);
        return answer.completeOnTimeout(null, kickTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .handle((wasOnline, e) -> {
                    kicks.remove(requestId);
                    if (wasOnline == null) {
                        LOG.info("Double login of {}: old proxy '{}' did not answer within {}, replacing",
                                player, oldProxy, kickTimeout);
                    } else {
                        LOG.debug("Double login of {}: old proxy '{}' answered was_online={}", player, oldProxy,
                                wasOnline);
                    }
                    return null;
                });
    }

    /** Creates the claimed entry (replacing any other) unless the claiming proxy is gone meanwhile. */
    private boolean put(String proxy, UUID player, PresenceEntry fresh) {
        List<Runnable> after = new ArrayList<>(1);
        boolean accepted;
        synchronized (lock) {
            if (registry.isShuttingDown()) return false;
            Slot prev = entries.get(player);
            boolean replaces = prev != null && !prev.entry().getProxy().equals(proxy);
            // Re-check after the (asynchronous) kick wait: if the claiming proxy has no open session now, its
            // node-gone has run or will run without seeing this entry, so it must not be created. A reconnected
            // proxy (the old session superseded) owns the login again and its reconciliation applies.
            accepted = openSession(proxy).isPresent();
            if (accepted) {
                entries.put(player, new Slot(fresh, clock.instant()));
                broadcast(PresenceChanged.newBuilder().setUpsert(fresh).build());
            } else if (replaces) {
                entries.remove(player);
                broadcast(PresenceChanged.newBuilder().setRemoved(player.toString()).build());
            }
            if (replaces) after.add(() -> events.removed(prev.entry(), RemovalReason.REPLACED));
        }
        if (!accepted) LOG.info("Claim of {} by '{}' dropped: the proxy's stream ended meanwhile", player, proxy);
        fire(after);
        return accepted;
    }

    // ---------- stream messages ----------

    @Override
    public void onKickResult(NodeSession session, KickResult result) {
        PendingKick k = kicks.get(result.getRequestId());
        if (k == null || !k.nodeId().equals(session.nodeId())) {
            LOG.debug("KickResult '{}' from '{}' matches no pending kick", result.getRequestId(), session.nodeId());
            return;
        }
        k.answer().complete(result.getWasOnline());
    }

    @Override
    public void onPlayerSwitched(NodeSession session, PlayerSwitched switched) {
        UUID player = parse(switched.getPlayer(), session);
        if (player == null) return;
        String proxy = session.nodeId();
        chain(player, () -> {
            applySwitch(proxy, player, switched.getServer());
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public void onPlayerLeft(NodeSession session, PlayerLeft left) {
        UUID player = parse(left.getPlayer(), session);
        if (player == null) return;
        String proxy = session.nodeId();
        chain(player, () -> {
            applyLeft(proxy, player);
            return CompletableFuture.completedFuture(null);
        });
    }

    private void applySwitch(String proxy, UUID player, String server) {
        List<Runnable> after = new ArrayList<>(1);
        synchronized (lock) {
            if (registry.isShuttingDown()) return;
            Slot slot = entries.get(player);
            if (slot == null || !slot.entry().getProxy().equals(proxy)) {
                LOG.debug("PlayerSwitched of {} from '{}' ignored: not the owning proxy", player, proxy);
                return;
            }
            String previous = slot.entry().getServer();
            if (previous.equals(server)) return;
            PresenceEntry updated = slot.entry().toBuilder().setServer(server).build();
            entries.put(player, new Slot(updated, slot.createdAt()));
            broadcast(PresenceChanged.newBuilder().setUpsert(updated).build());
            after.add(() -> events.switched(updated, previous));
        }
        fire(after);
    }

    private void applyLeft(String proxy, UUID player) {
        List<Runnable> after = new ArrayList<>(1);
        synchronized (lock) {
            if (registry.isShuttingDown()) return;
            Slot slot = entries.get(player);
            if (slot == null || !slot.entry().getProxy().equals(proxy)) {
                LOG.debug("PlayerLeft of {} from '{}' ignored: not the owning proxy", player, proxy);
                return;
            }
            entries.remove(player);
            broadcast(PresenceChanged.newBuilder().setRemoved(player.toString()).build());
            after.add(() -> events.removed(slot.entry(), RemovalReason.LEFT));
        }
        fire(after);
    }

    @Override
    public void onHello(NodeSession session, Hello hello) {
        if (session.kind() == NodeKind.VELOCITY) reconcile(session, hello.getPlayersList());
    }

    @Override
    public void onHeartbeat(NodeSession session, Heartbeat heartbeat) {
        if (session.kind() == NodeKind.VELOCITY) reconcile(session, heartbeat.getPlayersList());
    }

    /** Hello/Heartbeat of a proxy: its complete list of players. No events (audit_log.md §5). */
    private void reconcile(NodeSession session, List<PresenceEntry> players) {
        String proxy = session.nodeId();
        Instant now = clock.instant();
        Set<UUID> listed = new HashSet<>();
        synchronized (lock) {
            if (registry.isShuttingDown()) return;
            for (PresenceEntry p : players) {
                UUID player = parse(p.getPlayer(), session);
                if (player == null) continue;
                listed.add(player);
                if (chains.containsKey(player)) continue; // a claim/switch/left is in flight: it decides
                Slot slot = entries.get(player);
                if (slot == null) {
                    PresenceEntry added = p.toBuilder().setProxy(proxy).build();
                    entries.put(player, new Slot(added, now));
                    broadcast(PresenceChanged.newBuilder().setUpsert(added).build());
                } else if (!slot.entry().getProxy().equals(proxy)) {
                    LOG.debug("Proxy '{}' lists {}, but it belongs to '{}': ignored", proxy, player,
                            slot.entry().getProxy());
                } else if (!slot.entry().getServer().equals(p.getServer())) {
                    PresenceEntry updated = slot.entry().toBuilder().setServer(p.getServer()).build();
                    entries.put(player, new Slot(updated, slot.createdAt()));
                    broadcast(PresenceChanged.newBuilder().setUpsert(updated).build());
                }
            }
            for (Iterator<Map.Entry<UUID, Slot>> it = entries.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<UUID, Slot> e = it.next();
                Slot slot = e.getValue();
                if (!slot.entry().getProxy().equals(proxy) || listed.contains(e.getKey())) continue;
                if (chains.containsKey(e.getKey())) continue;
                if (Duration.between(slot.createdAt(), now).compareTo(RECONCILE_GRACE) < 0) continue; // E11
                it.remove();
                LOG.debug("Proxy '{}' no longer lists {}: removed", proxy, e.getKey());
                broadcast(PresenceChanged.newBuilder().setRemoved(e.getKey().toString()).build());
            }
        }
    }

    // ---------- node-gone ----------

    private void nodeGone(NodeSession session) {
        for (PendingKick k : kicks.values()) {
            if (k.nodeId().equals(session.nodeId())) k.answer().complete(Boolean.FALSE); // it will not answer
        }
        if (session.kind() != NodeKind.VELOCITY) return;
        if (registry.isSuperseded(session)) return; // R2: reconnected, the new session reconciles
        List<Runnable> after = new ArrayList<>();
        synchronized (lock) {
            if (registry.isShuttingDown()) return;
            for (Iterator<Map.Entry<UUID, Slot>> it = entries.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<UUID, Slot> e = it.next();
                if (!e.getValue().entry().getProxy().equals(session.nodeId())) continue;
                it.remove();
                PresenceEntry removed = e.getValue().entry();
                broadcast(PresenceChanged.newBuilder().setRemoved(e.getKey().toString()).build());
                after.add(() -> events.removed(removed, RemovalReason.PROXY_LOST));
            }
        }
        if (!after.isEmpty()) LOG.info("Proxy '{}' is gone, removed {} players", session.nodeId(), after.size());
        fire(after);
    }

    // ---------- snapshot, helpers ----------

    @Override
    public List<PresenceEntry> snapshot() {
        synchronized (lock) {
            List<PresenceEntry> out = new ArrayList<>(entries.size());
            for (Slot s : entries.values()) out.add(s.entry());
            return out;
        }
    }

    /** Completes once every claim/switch/left queued so far for {@code player} has run (tests, diagnostics). */
    public CompletableFuture<Void> barrier(UUID player) {
        CompletableFuture<?> tail = chains.get(player);
        return tail == null ? CompletableFuture.completedFuture(null) : tail.handle((v, e) -> null);
    }

    /** Runs {@code step} after every step queued before for {@code player} (claims, switches, leaves). */
    private <T> CompletableFuture<T> chain(UUID player, Supplier<CompletableFuture<T>> step) {
        CompletableFuture<T> result = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        CompletableFuture<?>[] prev = new CompletableFuture[1];
        chains.compute(player, (k, p) -> {
            prev[0] = p;
            return result;
        });
        result.whenComplete((v, e) -> chains.remove(player, result));
        CompletableFuture<?> base = prev[0] == null ? CompletableFuture.completedFuture(null) : prev[0];
        base.handle((v, e) -> null)
                .thenCompose(v -> {
                    try {
                        return step.get();
                    } catch (RuntimeException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                })
                .whenComplete((v, e) -> {
                    if (e != null) result.completeExceptionally(e);
                    else result.complete(v);
                });
        return result;
    }

    private Optional<NodeSession> openSession(String nodeId) {
        return registry.get(nodeId).filter(NodeSession::isOpen);
    }

    /** Sends {@code change} to every session. Called under {@link #lock}; {@code send} only queues. */
    private void broadcast(PresenceChanged change) {
        ServiceMessage m = ServiceMessage.newBuilder().setPresenceChanged(change).build();
        for (NodeSession s : registry.all()) s.send(m);
    }

    private static void fire(List<Runnable> after) {
        for (Runnable r : after) {
            try {
                r.run();
            } catch (RuntimeException e) {
                LOG.error("Presence event listener failed", e);
            }
        }
    }

    private static UUID parse(String player, NodeSession from) {
        try {
            return UUID.fromString(player);
        } catch (IllegalArgumentException e) {
            LOG.debug("Invalid player UUID '{}' from '{}' ignored", player, from.nodeId());
            return null;
        }
    }
}

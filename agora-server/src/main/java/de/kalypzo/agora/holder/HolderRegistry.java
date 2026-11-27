package de.kalypzo.agora.holder;

import de.kalypzo.agora.data.ChangePublisher;
import de.kalypzo.agora.data.HolderSink;
import de.kalypzo.agora.node.HelloListener;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.node.Subscription;
import de.kalypzo.agora.proto.v1.DocChanged;
import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.ReleasePlayer;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.proto.v1.UnwatchStore;
import de.kalypzo.agora.proto.v1.WatchStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Who receives which {@code DocChanged} (agora §5 "Halter"): player holders {@code UUID → nodes} and store watchers
 * {@code store → nodes}.
 *
 * <ul>
 *   <li>Sources: {@code LoadPlayer} ({@link #addPlayerHolder}, before the read), {@code ReleasePlayer} (E3),
 *       {@code WatchStore}/{@code UnwatchStore} (all immediate), and {@code Hello}/{@code Heartbeat} (E8): these
 *       <b>replace</b> the node's sets, except that entries younger than {@value #GRACE_SECONDS} s survive (R3, as
 *       E11), so a heartbeat built before a fresh {@code LoadPlayer} does not drop it. Symmetrically, a
 *       {@code ReleasePlayer}/{@code UnwatchStore} leaves a tombstone for {@value #GRACE_SECONDS} s that
 *       Hello/Heartbeat cannot override, so a heartbeat built before the release does not bring the entry back; a new
 *       explicit {@code LoadPlayer}/{@code WatchStore} clears it.
 *   <li>Node-gone of a session that is not superseded (R2): every entry of that node that is not newer than the end
 *       of the ended session is removed; a {@code LoadPlayer} in the gap between the end and node-gone keeps its
 *       entry (the next Hello/Heartbeat reconciles it). After shutdown (NOT_SERVING) the registry fires no
 *       node-gone, so nothing is removed.
 *   <li>{@link #publish}: a {@code PlayerRow} goes to every holder of the player, a {@code StoreDoc} to every watcher
 *       of its store, as {@code ServiceMessage.doc_changed} over the node's current session (the writer's own echo
 *       included, carrying the request key, E4). Nodes without an open session are skipped; they resync on
 *       reconnect. Never blocks: {@link NodeSession#send} only queues.
 * </ul>
 *
 * Thread-safe; one monitor guards all maps, sending happens outside of it. Register with {@link #install}.
 */
public final class HolderRegistry implements HolderSink, ChangePublisher, HelloListener, NodeMessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(HolderRegistry.class);

    static final int GRACE_SECONDS = 10;
    /** R3: Hello/Heartbeat never remove entries younger than this. */
    public static final Duration GRACE = Duration.ofSeconds(GRACE_SECONDS);

    /** What one node holds, with the time each entry was (last) added, and its tombstones (release time). */
    private static final class Holds {
        final Map<UUID, Instant> players = new HashMap<>();
        final Map<String, Instant> stores = new HashMap<>();
        final Map<UUID, Instant> releasedPlayers = new HashMap<>();
        final Map<String, Instant> unwatchedStores = new HashMap<>();

        boolean isEmpty() {
            return players.isEmpty() && stores.isEmpty() && releasedPlayers.isEmpty() && unwatchedStores.isEmpty();
        }
    }

    private final NodeRegistry nodes;
    private final Clock clock;
    private final Object lock = new Object();
    private final Map<String, Holds> byNode = new HashMap<>(); // guarded by lock
    private final Map<UUID, Set<String>> playerHolders = new HashMap<>(); // guarded by lock
    private final Map<String, Set<String>> storeWatchers = new HashMap<>(); // guarded by lock
    private final Map<String, Instant> sessionEnds = new HashMap<>(); // nodeId -> end of its last session, by lock
    private Subscription installation; // guarded by this
    private NodeMessageDispatcher installedOn; // guarded by this

    public HolderRegistry(NodeRegistry nodes, Clock clock) {
        this.nodes = nodes;
        this.clock = clock;
    }

    public HolderRegistry(NodeRegistry nodes) {
        this(nodes, Clock.systemUTC());
    }

    /**
     * Subscribes to the node messages (Hello, Heartbeat, Watch/Unwatch, Release), to session ends and to node-gone.
     * Idempotent for the same dispatcher (returns the existing subscription); a second, different dispatcher is
     * rejected with {@link IllegalStateException}.
     */
    public synchronized Subscription install(NodeMessageDispatcher dispatcher) {
        if (installation != null) {
            if (installedOn != dispatcher) throw new IllegalStateException("already installed on another dispatcher");
            return installation;
        }
        Subscription messages = dispatcher.addListener(this);
        Subscription ended = nodes.onSessionEnded(this::sessionEnded);
        Subscription gone = nodes.onNodeGone(this::nodeGone);
        installedOn = dispatcher;
        installation = () -> {
            messages.cancel();
            ended.cancel();
            gone.cancel();
        };
        return installation;
    }

    // ---------- queries ----------

    /** The nodes currently holding {@code player} (a copy). */
    public Set<String> holdersOf(UUID player) {
        synchronized (lock) {
            return Set.copyOf(playerHolders.getOrDefault(player, Set.of()));
        }
    }

    /** The nodes currently watching {@code store} ({@code <namespace>:<store>}, a copy). */
    public Set<String> watchersOf(String store) {
        synchronized (lock) {
            return Set.copyOf(storeWatchers.getOrDefault(store, Set.of()));
        }
    }

    // ---------- sources ----------

    @Override
    public void addPlayerHolder(String nodeId, UUID player) {
        Instant now = clock.instant();
        synchronized (lock) {
            addPlayer(nodeId, player, now);
            byNode.get(nodeId).releasedPlayers.remove(player); // newer than the tombstone
        }
    }

    @Override
    public void onReleasePlayer(NodeSession session, ReleasePlayer release) {
        UUID player = parse(session, release.getPlayer());
        if (player == null) return;
        Instant now = clock.instant();
        synchronized (lock) {
            Holds h = byNode.computeIfAbsent(session.nodeId(), id -> new Holds());
            if (h.players.remove(player) != null) unindexPlayer(session.nodeId(), player);
            h.releasedPlayers.put(player, now);
        }
    }

    @Override
    public void onWatchStore(NodeSession session, WatchStore watch) {
        if (watch.getStore().isEmpty()) return;
        Instant now = clock.instant();
        synchronized (lock) {
            addStore(session.nodeId(), watch.getStore(), now);
            byNode.get(session.nodeId()).unwatchedStores.remove(watch.getStore());
        }
    }

    @Override
    public void onUnwatchStore(NodeSession session, UnwatchStore unwatch) {
        if (unwatch.getStore().isEmpty()) return;
        Instant now = clock.instant();
        synchronized (lock) {
            Holds h = byNode.computeIfAbsent(session.nodeId(), id -> new Holds());
            if (h.stores.remove(unwatch.getStore()) != null) unindexStore(session.nodeId(), unwatch.getStore());
            h.unwatchedStores.put(unwatch.getStore(), now);
        }
    }

    @Override
    public void onHello(NodeSession session, Hello hello) {
        reconcile(session, hello.getHeldPlayersList(), hello.getWatchedStoresList());
    }

    @Override
    public void onHeartbeat(NodeSession session, Heartbeat heartbeat) {
        reconcile(session, heartbeat.getHeldPlayersList(), heartbeat.getWatchedStoresList());
    }

    /**
     * Replaces the node's sets by the listed ones; unlisted entries younger than {@link #GRACE} stay (R3), listed
     * ones released/unwatched less than {@link #GRACE} ago are not added (tombstone).
     */
    private void reconcile(NodeSession session, List<String> heldPlayers, List<String> watchedStores) {
        String nodeId = session.nodeId();
        Set<UUID> players = new HashSet<>();
        for (String s : heldPlayers) {
            UUID p = parse(session, s);
            if (p != null) players.add(p);
        }
        Set<String> stores = new HashSet<>();
        for (String s : watchedStores) {
            if (!s.isEmpty()) stores.add(s);
        }
        Instant now = clock.instant();
        Instant staleBefore = now.minus(GRACE);
        synchronized (lock) {
            Holds h = byNode.get(nodeId);
            if (h != null) {
                h.releasedPlayers.values().removeIf(at -> !at.isAfter(staleBefore));
                h.unwatchedStores.values().removeIf(at -> !at.isAfter(staleBefore));
                for (Iterator<Map.Entry<UUID, Instant>> it = h.players.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<UUID, Instant> e = it.next();
                    if (!players.contains(e.getKey()) && !e.getValue().isAfter(staleBefore)) {
                        it.remove();
                        unindexPlayer(nodeId, e.getKey());
                    }
                }
                for (Iterator<Map.Entry<String, Instant>> it = h.stores.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<String, Instant> e = it.next();
                    if (!stores.contains(e.getKey()) && !e.getValue().isAfter(staleBefore)) {
                        it.remove();
                        unindexStore(nodeId, e.getKey());
                    }
                }
            }
            Holds cur = byNode.computeIfAbsent(nodeId, id -> new Holds());
            for (UUID p : players) {
                if (!cur.players.containsKey(p) && !cur.releasedPlayers.containsKey(p)) addPlayer(nodeId, p, now);
            }
            for (String s : stores) {
                if (!cur.stores.containsKey(s) && !cur.unwatchedStores.containsKey(s)) addStore(nodeId, s, now);
            }
            dropIfEmpty(nodeId);
        }
    }

    private void sessionEnded(NodeSession session) {
        Instant now = clock.instant();
        synchronized (lock) {
            sessionEnds.put(session.nodeId(), now);
        }
    }

    private void nodeGone(NodeSession session) {
        // Called by the registry only for a non-superseded session and never after shutdown (R2).
        String nodeId = session.nodeId();
        int kept = 0;
        synchronized (lock) {
            // Recorded synchronously before node-gone was queued; the fallback is only for a missing record.
            Instant end = sessionEnds.remove(nodeId);
            if (end == null) end = clock.instant();
            Holds h = byNode.get(nodeId);
            if (h == null) return;
            Instant until = end;
            for (Iterator<Map.Entry<UUID, Instant>> it = h.players.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<UUID, Instant> e = it.next();
                if (e.getValue().isAfter(until)) continue; // LoadPlayer in the gap: newer than the session end
                it.remove();
                unindexPlayer(nodeId, e.getKey());
            }
            for (Iterator<Map.Entry<String, Instant>> it = h.stores.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Instant> e = it.next();
                if (e.getValue().isAfter(until)) continue;
                it.remove();
                unindexStore(nodeId, e.getKey());
            }
            // Tombstones belong to the ended session's messages, all handled by now; a new session's Hello is
            // built after them.
            h.releasedPlayers.clear();
            h.unwatchedStores.clear();
            kept = h.players.size() + h.stores.size();
            dropIfEmpty(nodeId);
        }
        LOG.debug("Node '{}' is gone, its holder entries were removed ({} newer ones kept)", nodeId, kept);
    }

    // ---------- routing ----------

    @Override
    public void publish(DocChanged change) {
        List<String> targets;
        synchronized (lock) {
            Set<String> set = switch (change.getRef().getTargetCase()) {
                case PLAYER_ROW -> {
                    UUID p = parseQuietly(change.getRef().getPlayerRow().getPlayer());
                    yield p == null ? null : playerHolders.get(p);
                }
                case STORE_DOC -> storeWatchers.get(change.getRef().getStoreDoc().getStore());
                case TARGET_NOT_SET -> null;
            };
            if (set == null || set.isEmpty()) return;
            targets = new ArrayList<>(set);
        }
        ServiceMessage message = ServiceMessage.newBuilder().setDocChanged(change).build();
        for (String nodeId : targets) {
            nodes.get(nodeId).ifPresent(s -> s.send(message));
        }
    }

    // ---------- internals (under lock) ----------

    private void addPlayer(String nodeId, UUID player, Instant at) {
        byNode.computeIfAbsent(nodeId, id -> new Holds()).players.put(player, at);
        playerHolders.computeIfAbsent(player, p -> new HashSet<>()).add(nodeId);
    }

    private void addStore(String nodeId, String store, Instant at) {
        byNode.computeIfAbsent(nodeId, id -> new Holds()).stores.put(store, at);
        storeWatchers.computeIfAbsent(store, s -> new HashSet<>()).add(nodeId);
    }

    private void unindexPlayer(String nodeId, UUID player) {
        Set<String> set = playerHolders.get(player);
        if (set != null && set.remove(nodeId) && set.isEmpty()) playerHolders.remove(player);
    }

    private void unindexStore(String nodeId, String store) {
        Set<String> set = storeWatchers.get(store);
        if (set != null && set.remove(nodeId) && set.isEmpty()) storeWatchers.remove(store);
    }

    private void dropIfEmpty(String nodeId) {
        Holds h = byNode.get(nodeId);
        if (h != null && h.isEmpty()) byNode.remove(nodeId);
    }

    private static UUID parse(NodeSession session, String s) {
        UUID p = parseQuietly(s);
        if (p == null) LOG.warn("Node '{}' sent an invalid player UUID '{}', ignoring it", session.nodeId(), s);
        return p;
    }

    private static UUID parseQuietly(String s) {
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}

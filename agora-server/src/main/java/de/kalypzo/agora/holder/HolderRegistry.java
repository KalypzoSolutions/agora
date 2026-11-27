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
 *       E11), so a heartbeat built before a fresh {@code LoadPlayer} does not drop it.
 *   <li>Node-gone of a session that is not superseded (R2): every entry of that node is removed. After shutdown
 *       (NOT_SERVING) the registry fires no node-gone, so nothing is removed.
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

    /** What one node holds, with the time each entry was (last) added. */
    private static final class Holds {
        final Map<UUID, Instant> players = new HashMap<>();
        final Map<String, Instant> stores = new HashMap<>();

        boolean isEmpty() {
            return players.isEmpty() && stores.isEmpty();
        }
    }

    private final NodeRegistry nodes;
    private final Clock clock;
    private final Object lock = new Object();
    private final Map<String, Holds> byNode = new HashMap<>(); // guarded by lock
    private final Map<UUID, Set<String>> playerHolders = new HashMap<>(); // guarded by lock
    private final Map<String, Set<String>> storeWatchers = new HashMap<>(); // guarded by lock

    public HolderRegistry(NodeRegistry nodes, Clock clock) {
        this.nodes = nodes;
        this.clock = clock;
    }

    public HolderRegistry(NodeRegistry nodes) {
        this(nodes, Clock.systemUTC());
    }

    /** Subscribes to the node messages (Hello, Heartbeat, Watch/Unwatch, Release) and to node-gone. */
    public Subscription install(NodeMessageDispatcher dispatcher) {
        Subscription messages = dispatcher.addListener(this);
        Subscription gone = nodes.onNodeGone(this::nodeGone);
        return () -> {
            messages.cancel();
            gone.cancel();
        };
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
        }
    }

    @Override
    public void onReleasePlayer(NodeSession session, ReleasePlayer release) {
        UUID player = parse(session, release.getPlayer());
        if (player == null) return;
        synchronized (lock) {
            Holds h = byNode.get(session.nodeId());
            if (h != null && h.players.remove(player) != null) unindexPlayer(session.nodeId(), player);
            dropIfEmpty(session.nodeId());
        }
    }

    @Override
    public void onWatchStore(NodeSession session, WatchStore watch) {
        if (watch.getStore().isEmpty()) return;
        Instant now = clock.instant();
        synchronized (lock) {
            addStore(session.nodeId(), watch.getStore(), now);
        }
    }

    @Override
    public void onUnwatchStore(NodeSession session, UnwatchStore unwatch) {
        synchronized (lock) {
            Holds h = byNode.get(session.nodeId());
            if (h != null && h.stores.remove(unwatch.getStore()) != null) {
                unindexStore(session.nodeId(), unwatch.getStore());
            }
            dropIfEmpty(session.nodeId());
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

    /** Replaces the node's sets by the listed ones; unlisted entries younger than {@link #GRACE} stay (R3). */
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
            for (UUID p : players) {
                Holds cur = byNode.get(nodeId);
                if (cur == null || !cur.players.containsKey(p)) addPlayer(nodeId, p, now);
            }
            for (String s : stores) {
                Holds cur = byNode.get(nodeId);
                if (cur == null || !cur.stores.containsKey(s)) addStore(nodeId, s, now);
            }
            dropIfEmpty(nodeId);
        }
    }

    private void nodeGone(NodeSession session) {
        // Called by the registry only for a non-superseded session and never after shutdown (R2).
        String nodeId = session.nodeId();
        synchronized (lock) {
            Holds h = byNode.remove(nodeId);
            if (h == null) return;
            for (UUID p : h.players.keySet()) unindexPlayer(nodeId, p);
            for (String s : h.stores.keySet()) unindexStore(nodeId, s);
        }
        LOG.debug("Node '{}' is gone, its holder entries were removed", nodeId);
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

package de.kalypzo.agora.node;

import io.grpc.Status;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The open sessions, one per node id. A new session with the same id replaces the old one, which is closed with
 * {@code ABORTED} ("replaced").
 *
 * <p>Node-gone (R2): the listeners of {@link #onNodeGone} are called only when the ending session is still the
 * registered session for its node id. A replaced session never fires it, and after {@link #shutdown} nothing fires.
 */
public final class NodeRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(NodeRegistry.class);

    private final ConcurrentHashMap<String, NodeSession> sessions = new ConcurrentHashMap<>();
    private final List<Consumer<NodeSession>> goneListeners = new CopyOnWriteArrayList<>();
    private volatile boolean shuttingDown;

    /**
     * Registers {@code s}; an existing session with the same id is closed with {@code ABORTED}. After
     * {@link #shutdown} the session is closed with {@code UNAVAILABLE} instead.
     */
    public void register(NodeSession s) {
        if (shuttingDown) {
            s.close(Status.UNAVAILABLE.withDescription("shutting down"));
            return;
        }
        NodeSession previous = sessions.put(s.nodeId(), s);
        s.closeFuture().whenComplete((status, err) -> ended(s));
        if (previous != null && previous != s) {
            LOG.info("Node '{}' reconnected, replacing its previous session", s.nodeId());
            previous.close(Status.ABORTED.withDescription("replaced"));
        }
        if (shuttingDown) s.close(Status.UNAVAILABLE.withDescription("shutting down"));
    }

    public Optional<NodeSession> get(String nodeId) {
        return Optional.ofNullable(sessions.get(nodeId));
    }

    /** {@code true} if {@code s} is the currently registered session for its node id. */
    public boolean isCurrent(NodeSession s) {
        return sessions.get(s.nodeId()) == s;
    }

    public Collection<NodeSession> all() {
        return List.copyOf(sessions.values());
    }

    /** Called with the ended session when a node is gone (R2, see class doc). */
    public Subscription onNodeGone(Consumer<NodeSession> listener) {
        goneListeners.add(listener);
        return () -> goneListeners.remove(listener);
    }

    /** Closes every session with {@code status} (A-14: UNAVAILABLE) without node-gone; rejects new sessions. */
    public void shutdown(Status status) {
        shuttingDown = true;
        for (NodeSession s : sessions.values()) {
            s.close(status);
        }
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    private void ended(NodeSession s) {
        if (!sessions.remove(s.nodeId(), s)) return; // replaced: the new session owns the node id
        if (shuttingDown) return;
        LOG.info("Node '{}' is gone", s.nodeId());
        for (Consumer<NodeSession> l : goneListeners) {
            try {
                l.accept(s);
            } catch (RuntimeException e) {
                LOG.error("Node-gone listener failed for '{}'", s.nodeId(), e);
            }
        }
    }
}

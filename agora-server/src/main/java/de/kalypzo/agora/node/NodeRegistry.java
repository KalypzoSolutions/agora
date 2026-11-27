package de.kalypzo.agora.node;

import io.grpc.Status;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The open sessions, one per node id. A new session with the same id replaces the old one, which is closed with
 * {@code ABORTED} ("replaced").
 *
 * <p><b>Lanes.</b> Everything that changes per-node state runs on a serial lane per node id ({@link #serialize}),
 * off the stream threads and in arrival order: the accepted {@code Hello} (listeners and Welcome), every further
 * {@code NodeMessage} of the node's sessions, and node-gone. Sessions are registered on the lane too (NodeStream), so
 * a reconnect's registration is ordered with the previous session's node-gone. During shutdown, queued tasks skip
 * their work; {@link #close} waits for them. Two sessions of the same node id therefore never run
 * their handlers in parallel.
 *
 * <p><b>Node-gone (R2).</b> Each registered session gets an increasing {@linkplain NodeSession#generation()
 * generation}; {@link #isSuperseded} tells whether a newer session for the same node id exists or existed. Node-gone
 * listeners are called only for a session that ended while it was registered and that is still not superseded when
 * its lane task runs (a node that reconnected before its node-gone was processed is not gone). After
 * {@link #shutdown} nothing fires. Listeners that continue asynchronously should re-check {@link #isSuperseded} before
 * applying deferred work. Inside a node-gone listener {@link #isCurrent} is always false (the session is already
 * removed); use {@link #isSuperseded} instead.
 */
public final class NodeRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(NodeRegistry.class);

    private final Executor executor;
    private final ExecutorService ownedExecutor; // closed by close(); null if the executor was injected
    private final ConcurrentHashMap<String, AtomicInteger> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodeSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> latestGeneration = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> lanes = new ConcurrentHashMap<>();
    private final AtomicLong generations = new AtomicLong();
    private final List<Consumer<NodeSession>> goneListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<NodeSession>> endListeners = new CopyOnWriteArrayList<>();
    private volatile boolean shuttingDown;

    /** @param executor runs the lane tasks (serialized per node id by the registry) */
    public NodeRegistry(Executor executor) {
        this.executor = executor;
        this.ownedExecutor = null;
    }

    /** Lanes on virtual threads; the executor is owned by the registry and closed by {@link #close}. */
    public NodeRegistry() {
        ExecutorService own = Executors.newVirtualThreadPerTaskExecutor();
        this.executor = own;
        this.ownedExecutor = own;
    }

    /**
     * Registers {@code s}; an existing session with the same id is closed with {@code ABORTED}. After
     * {@link #shutdown} the session is closed with {@code UNAVAILABLE} instead.
     */
    public void register(NodeSession s) {
        if (shuttingDown) {
            s.close(Status.UNAVAILABLE.withDescription("shutting down"));
            return;
        }
        NodeSession[] previous = new NodeSession[1];
        sessions.compute(s.nodeId(), (id, prev) -> {
            previous[0] = prev;
            long gen = generations.incrementAndGet();
            s.generation = gen;
            latestGeneration.put(id, gen);
            return s;
        });
        s.closeFuture().whenComplete((status, err) -> ended(s));
        if (previous[0] != null && previous[0] != s) {
            LOG.info("Node '{}' reconnected, replacing its previous session", s.nodeId());
            previous[0].close(Status.ABORTED.withDescription("replaced"));
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

    /** {@code true} if a session for the same node id was registered after {@code s} (whether still open or not). */
    public boolean isSuperseded(NodeSession s) {
        Long latest = latestGeneration.get(s.nodeId());
        return latest != null && latest > s.generation();
    }

    public Collection<NodeSession> all() {
        return List.copyOf(sessions.values());
    }

    /** Called with the ended session when a node is gone, on the node's lane (R2, see class doc). */
    public Subscription onNodeGone(Consumer<NodeSession> listener) {
        goneListeners.add(listener);
        return () -> goneListeners.remove(listener);
    }

    /**
     * Called synchronously, on the thread that ended it, when the registered (not replaced) session of a node ends,
     * before its node-gone is queued and before the session is removed from {@link #get}. Lets a node-gone listener
     * tell what happened after the end (A-10: a LoadPlayer in the gap). Must not block.
     */
    public Subscription onSessionEnded(Consumer<NodeSession> listener) {
        endListeners.add(listener);
        return () -> endListeners.remove(listener);
    }

    /**
     * Runs {@code task} on the serial lane of {@code nodeId}, after every task queued before for that node id.
     * Exceptions are logged. The returned future completes when the task has run.
     */
    public CompletableFuture<Void> serialize(String nodeId, Runnable task) {
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] next = new CompletableFuture[1];
        AtomicInteger counter = pending.computeIfAbsent(nodeId, id -> new AtomicInteger());
        counter.incrementAndGet();
        lanes.compute(nodeId, (id, prev) -> {
            CompletableFuture<Void> base = prev == null ? CompletableFuture.completedFuture(null) : prev;
            next[0] = base.handle((v, e) -> null).thenRunAsync(() -> {
                try {
                    runSafely(nodeId, task);
                } finally {
                    counter.decrementAndGet();
                }
            }, executor);
            return next[0];
        });
        CompletableFuture<Void> f = next[0];
        f.whenComplete((v, e) -> lanes.remove(nodeId, f));
        return f;
    }

    /** Lane tasks of {@code nodeId} that are queued or running (for tests and diagnostics). */
    public int pending(String nodeId) {
        AtomicInteger c = pending.get(nodeId);
        return c == null ? 0 : c.get();
    }

    /**
     * Ends the registry: {@link #shutdown} with {@code UNAVAILABLE} (if not done yet), then waits up to
     * {@code timeout} for the queued lane tasks (they skip their work during shutdown), then closes the owned
     * executor. Call before closing resources the listeners use (the DB pool). Returns {@code false} on timeout.
     */
    public boolean close(Duration timeout) {
        if (!shuttingDown) shutdown(Status.UNAVAILABLE.withDescription("shutting down"));
        boolean drained = true;
        try {
            CompletableFuture.allOf(lanes.values().toArray(CompletableFuture[]::new))
                    .handle((v, e) -> null)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            drained = false;
            LOG.warn("Node lanes did not finish within {}", timeout);
        } catch (InterruptedException e) {
            drained = false;
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // handle() swallows task failures
        }
        if (ownedExecutor != null) ownedExecutor.shutdownNow();
        return drained;
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
        if (sessions.get(s.nodeId()) != s) return; // replaced: the new session owns the node id
        for (Consumer<NodeSession> l : endListeners) {
            try {
                l.accept(s);
            } catch (RuntimeException e) {
                LOG.error("Session-end listener failed for '{}'", s.nodeId(), e);
            }
        }
        if (!sessions.remove(s.nodeId(), s)) return; // replaced meanwhile
        if (shuttingDown) return;
        serialize(s.nodeId(), () -> {
            if (shuttingDown) return;
            if (isSuperseded(s)) {
                LOG.debug("Node '{}' reconnected before its previous session's node-gone ran, skipping", s.nodeId());
                return;
            }
            LOG.info("Node '{}' is gone", s.nodeId());
            for (Consumer<NodeSession> l : goneListeners) {
                try {
                    l.accept(s);
                } catch (RuntimeException e) {
                    LOG.error("Node-gone listener failed for '{}'", s.nodeId(), e);
                }
            }
        });
    }

    private static void runSafely(String nodeId, Runnable task) {
        try {
            task.run();
        } catch (RuntimeException e) {
            LOG.error("Lane task for node '{}' failed", nodeId, e);
        }
    }
}

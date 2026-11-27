package de.kalypzo.agora.node;

import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One open {@code Connect} stream of a node, after its {@code Hello} was accepted.
 *
 * <p>Outgoing messages go through a bounded queue. They are written to the stream only while
 * {@link ServerCallStreamObserver#isReady()} is true; the stream's on-ready handler must call {@link #drain()}. If
 * the queue would grow beyond its limit (default {@value #DEFAULT_QUEUE_LIMIT}), the session is closed with
 * {@code RESOURCE_EXHAUSTED}: the node reconnects and resyncs (agora §3, Backpressure). The limit counts messages
 * not yet handed to the transport; on Netty the transport additionally buffers up to its on-ready threshold and the
 * HTTP/2 window, so the limit approximates "unread" (agora §3: "z. B. 10.000").
 *
 * <p>Thread-safe: {@link #send} may be called from any thread. No callback (node-gone etc.) ever runs while the
 * session's monitor is held: {@link #closeFuture()} completes outside of it.
 */
public final class NodeSession {

    private static final Logger LOG = LoggerFactory.getLogger(NodeSession.class);

    /** Backpressure limit: unread messages per node (globals). */
    public static final int DEFAULT_QUEUE_LIMIT = 10_000;

    private final String nodeId;
    private final NodeKind kind;
    private final Instant connectedAt;
    private final StreamObserver<ServiceMessage> out;
    private final ServerCallStreamObserver<ServiceMessage> flow;
    private final int queueLimit;
    private final ArrayDeque<ServiceMessage> queue = new ArrayDeque<>();
    private final CompletableFuture<Status> closed = new CompletableFuture<>();
    private boolean done; // guarded by this
    volatile long generation; // set by NodeRegistry.register

    public NodeSession(String nodeId, NodeKind kind, Instant connectedAt, StreamObserver<ServiceMessage> out,
            int queueLimit) {
        if (queueLimit < 1) throw new IllegalArgumentException("queueLimit must be >= 1: " + queueLimit);
        this.nodeId = nodeId;
        this.kind = kind;
        this.connectedAt = connectedAt;
        this.out = out;
        this.flow = out instanceof ServerCallStreamObserver<ServiceMessage> s ? s : null;
        this.queueLimit = queueLimit;
    }

    public NodeSession(String nodeId, NodeKind kind, Instant connectedAt, StreamObserver<ServiceMessage> out) {
        this(nodeId, kind, connectedAt, out, DEFAULT_QUEUE_LIMIT);
    }

    public String nodeId() {
        return nodeId;
    }

    public NodeKind kind() {
        return kind;
    }

    public Instant connectedAt() {
        return connectedAt;
    }

    /** Registration order of this session among all sessions (0 = never registered). See NodeRegistry#isSuperseded. */
    public long generation() {
        return generation;
    }

    /**
     * Queues {@code m} for the node. Returns {@code false} if the session is closed, or if the queue is full; in the
     * latter case the session is closed with {@code RESOURCE_EXHAUSTED}.
     */
    public boolean send(ServiceMessage m) {
        RuntimeException writeFailure;
        int queued;
        synchronized (this) {
            if (done) return false;
            queued = queue.size();
            if (queued < queueLimit) {
                queue.add(m);
                writeFailure = drainLocked();
                if (writeFailure == null) return true;
            } else {
                writeFailure = null;
            }
        }
        // Decided under the lock, closed after releasing it (no callbacks under the monitor).
        if (writeFailure != null) {
            failedWrite(writeFailure);
            return false;
        }
        LOG.warn("Node '{}' does not read fast enough ({} queued messages), closing its stream", nodeId, queued);
        close(Status.RESOURCE_EXHAUSTED.withDescription("backpressure limit of " + queueLimit + " messages"));
        return false;
    }

    /** Writes queued messages while the stream is ready. Called by the on-ready handler of the stream. */
    public void drain() {
        RuntimeException writeFailure;
        synchronized (this) {
            writeFailure = drainLocked();
        }
        if (writeFailure != null) failedWrite(writeFailure);
    }

    private RuntimeException drainLocked() {
        try {
            while (!done && !queue.isEmpty() && (flow == null || flow.isReady())) {
                out.onNext(queue.poll());
            }
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    private void failedWrite(RuntimeException e) {
        LOG.debug("Writing to node '{}' failed: {}", nodeId, e.toString());
        peerClosed(Status.fromThrowable(e));
    }

    /** Number of queued, not yet written messages. */
    public synchronized int queued() {
        return queue.size();
    }

    /**
     * Ends the session and the stream: OK completes it, any other status fails it. Queued messages are dropped.
     * Idempotent.
     */
    public void close(Status status) {
        synchronized (this) {
            if (done) return;
            done = true;
            queue.clear();
            try {
                if (status.isOk()) out.onCompleted();
                else out.onError(status.asRuntimeException());
            } catch (RuntimeException e) {
                LOG.debug("Closing stream of node '{}' failed: {}", nodeId, e.toString());
            }
        }
        closed.complete(status);
    }

    /** The peer ended the stream (cancel, error): ends the session without writing to the stream. Idempotent. */
    public void peerClosed(Status status) {
        synchronized (this) {
            if (done) return;
            done = true;
            queue.clear();
        }
        closed.complete(status);
    }

    public synchronized boolean isOpen() {
        return !done;
    }

    /** Completes with the closing status once the session has ended, whichever side ended it. */
    public CompletableFuture<Status> closeFuture() {
        return closed.copy();
    }

    @Override
    public String toString() {
        return "NodeSession[" + nodeId + ", " + kind + ", gen " + generation + "]";
    }
}

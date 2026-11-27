package de.kalypzo.agora.node;

import de.kalypzo.agora.proto.v1.Barrier;
import de.kalypzo.agora.proto.v1.BarrierAck;
import de.kalypzo.agora.proto.v1.ServiceMessage;

/**
 * Stream barrier (R45): answers {@code Barrier(request_id)} with {@code BarrierAck(request_id)} on the same session.
 * The barrier runs on the node's lane like every other message, so the ack is queued only after every message the
 * node sent before it was handled (for example a {@code ReleasePlayer}). A node uses it before a {@code LoadPlayer}
 * that must not be overtaken by its own earlier release.
 */
public final class StreamBarrier implements NodeMessageListener {

    private StreamBarrier() {}

    /** Installs the responder on {@code dispatcher}. */
    public static Subscription install(NodeMessageDispatcher dispatcher) {
        return dispatcher.addListener(new StreamBarrier());
    }

    @Override
    public void onBarrier(NodeSession session, Barrier barrier) {
        session.send(ServiceMessage.newBuilder()
                .setBarrierAck(BarrierAck.newBuilder().setRequestId(barrier.getRequestId()))
                .build());
    }
}

package de.kalypzo.agora.node;

import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.NodeMessage;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes every {@code NodeMessage} payload to the registered {@link NodeMessageListener}s (R5). Listeners can be
 * added at any time and independently of each other; a failing listener is logged and does not affect the others or
 * the stream.
 */
public final class NodeMessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(NodeMessageDispatcher.class);

    private final List<NodeMessageListener> listeners = new CopyOnWriteArrayList<>();

    public Subscription addListener(NodeMessageListener listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    public Subscription addHelloListener(HelloListener listener) {
        return addListener(new NodeMessageListener() {
            @Override
            public void onHello(NodeSession session, Hello hello) {
                listener.onHello(session, hello);
            }
        });
    }

    /** Calls every listener for the payload of {@code message}. A message without payload is ignored. */
    public void dispatch(NodeSession session, NodeMessage message) {
        for (NodeMessageListener l : listeners) {
            try {
                deliver(l, session, message);
            } catch (RuntimeException e) {
                LOG.error("Listener {} failed on {} from node '{}'", l, message.getPayloadCase(), session.nodeId(), e);
            }
        }
    }

    private static void deliver(NodeMessageListener l, NodeSession s, NodeMessage m) {
        // Switch expression: a new payload in the proto fails to compile here until it is routed (R5).
        Runnable call = switch (m.getPayloadCase()) {
            case HELLO -> () -> l.onHello(s, m.getHello());
            case HEARTBEAT -> () -> l.onHeartbeat(s, m.getHeartbeat());
            case WATCH_STORE -> () -> l.onWatchStore(s, m.getWatchStore());
            case UNWATCH_STORE -> () -> l.onUnwatchStore(s, m.getUnwatchStore());
            case KICK_RESULT -> () -> l.onKickResult(s, m.getKickResult());
            case PLAYER_SWITCHED -> () -> l.onPlayerSwitched(s, m.getPlayerSwitched());
            case PLAYER_LEFT -> () -> l.onPlayerLeft(s, m.getPlayerLeft());
            case LOCATION_REPLY -> () -> l.onLocationReply(s, m.getLocationReply());
            case SEND_DELIVERY -> () -> l.onSendDelivery(s, m.getSendDelivery());
            case RELEASE_PLAYER -> () -> l.onReleasePlayer(s, m.getReleasePlayer());
            case PAYLOAD_NOT_SET -> () -> {};
        };
        call.run();
    }
}

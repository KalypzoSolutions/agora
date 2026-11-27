package de.kalypzo.agora.delivery;

import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeMessageListener;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.node.Subscription;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.proto.v1.Deliver;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.SendDelivery;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Messages to players (agora §6, E2): a node sends a rendered {@code SendDelivery} over its stream; the router looks
 * up the player's presence entry and forwards it as {@code Deliver} to the session of {@code entry.proxy}, the proxy
 * delivers it over Velocity. If the player is offline or the proxy has no open session, the message is dropped with a
 * DEBUG log line. Fire-and-forget: there is no answer.
 *
 * <p>Order: the messages of one sender run on its lane in stream order, and {@link NodeSession#send} queues in call
 * order, so the deliveries of one sender reach the proxy in the order they were sent. Never blocks (lookup under the
 * presence monitor, {@code send} only queues). Register with {@link #install}.
 */
public final class DeliveryRouter implements NodeMessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(DeliveryRouter.class);

    private final NodeRegistry nodes;
    private final PresenceService presence;

    public DeliveryRouter(NodeRegistry nodes, PresenceService presence) {
        this.nodes = nodes;
        this.presence = presence;
    }

    /** Subscribes to {@code SendDelivery} (R5). */
    public Subscription install(NodeMessageDispatcher dispatcher) {
        return dispatcher.addListener(this);
    }

    @Override
    public void onSendDelivery(NodeSession session, SendDelivery delivery) {
        UUID player;
        try {
            player = UUID.fromString(delivery.getPlayer());
        } catch (IllegalArgumentException e) {
            LOG.warn("Node '{}' sent a delivery to an invalid player UUID '{}', dropping it", session.nodeId(),
                    delivery.getPlayer());
            return;
        }
        Optional<NodeSession> proxy = presence.entry(player)
                .map(PresenceEntry::getProxy)
                .flatMap(nodes::get)
                .filter(NodeSession::isOpen);
        boolean sent = proxy.isPresent() && proxy.get().send(ServiceMessage.newBuilder()
                .setDeliver(Deliver.newBuilder().setPlayer(delivery.getPlayer()).setDelivery(delivery.getDelivery()))
                .build());
        if (!sent) LOG.debug("Zustellung an {} verworfen: offline", player);
    }
}

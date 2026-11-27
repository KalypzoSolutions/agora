package de.kalypzo.agora.node;

import de.kalypzo.agora.proto.v1.Heartbeat;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.KickResult;
import de.kalypzo.agora.proto.v1.LocationReply;
import de.kalypzo.agora.proto.v1.PlayerLeft;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.ReleasePlayer;
import de.kalypzo.agora.proto.v1.SendDelivery;
import de.kalypzo.agora.proto.v1.UnwatchStore;
import de.kalypzo.agora.proto.v1.WatchStore;

/**
 * One method per {@code NodeMessage} payload (R5). Every method defaults to a no-op, so a component overrides only
 * what it handles. Methods are called on the node's serial lane ({@link NodeRegistry#serialize}), off the stream
 * thread, in the order the node sent the messages; messages of a superseded session are no longer delivered. A
 * listener that blocks holds up only that node's lane, but should still not wait long; the gRPC context is not
 * available there.
 */
public interface NodeMessageListener {

    default void onHello(NodeSession session, Hello hello) {}

    default void onHeartbeat(NodeSession session, Heartbeat heartbeat) {}

    default void onWatchStore(NodeSession session, WatchStore watch) {}

    default void onUnwatchStore(NodeSession session, UnwatchStore unwatch) {}

    default void onKickResult(NodeSession session, KickResult result) {}

    default void onPlayerSwitched(NodeSession session, PlayerSwitched switched) {}

    default void onPlayerLeft(NodeSession session, PlayerLeft left) {}

    default void onLocationReply(NodeSession session, LocationReply reply) {}

    default void onSendDelivery(NodeSession session, SendDelivery delivery) {}

    default void onReleasePlayer(NodeSession session, ReleasePlayer release) {}
}

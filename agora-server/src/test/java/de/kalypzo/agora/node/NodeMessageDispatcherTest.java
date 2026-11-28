package de.kalypzo.agora.node;

import static org.assertj.core.api.Assertions.assertThat;

import de.kalypzo.agora.proto.v1.ArrivalFailed;
import de.kalypzo.agora.proto.v1.ArrivalPrepared;
import de.kalypzo.agora.proto.v1.ConnectResult;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PlayerArrived;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class NodeMessageDispatcherTest {

    @Test
    void routesTeleportMessages() {
        List<String> seen = new ArrayList<>();
        NodeMessageDispatcher dispatcher = new NodeMessageDispatcher();
        dispatcher.addListener(new NodeMessageListener() {
            @Override
            public void onArrivalPrepared(NodeSession session, ArrivalPrepared m) {
                seen.add("arrivalPrepared:" + m.getRequestId());
            }

            @Override
            public void onPlayerArrived(NodeSession session, PlayerArrived m) {
                seen.add("playerArrived:" + m.getRequestId());
            }

            @Override
            public void onArrivalFailed(NodeSession session, ArrivalFailed m) {
                seen.add("arrivalFailed:" + m.getRequestId());
            }

            @Override
            public void onConnectResult(NodeSession session, ConnectResult m) {
                seen.add("connectResult:" + m.getRequestId());
            }
        });
        NodeSession session = new NodeSession("lobby-01", NodeKind.PAPER, Instant.now(), new StreamObserver<>() {
            @Override public void onNext(ServiceMessage value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });

        dispatcher.dispatch(session, NodeMessage.newBuilder()
                .setArrivalPrepared(ArrivalPrepared.newBuilder().setRequestId("r1")).build());
        dispatcher.dispatch(session, NodeMessage.newBuilder()
                .setPlayerArrived(PlayerArrived.newBuilder().setRequestId("r2")).build());
        dispatcher.dispatch(session, NodeMessage.newBuilder()
                .setArrivalFailed(ArrivalFailed.newBuilder().setRequestId("r3")).build());
        dispatcher.dispatch(session, NodeMessage.newBuilder()
                .setConnectResult(ConnectResult.newBuilder().setRequestId("r4")).build());

        assertThat(seen).containsExactly("arrivalPrepared:r1", "playerArrived:r2", "arrivalFailed:r3",
                "connectResult:r4");
    }
}

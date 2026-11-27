package de.kalypzo.agora;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.holder.HolderRegistry;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.PlayerLeft;
import de.kalypzo.agora.proto.v1.PlayerSwitched;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-19: the session entries reach Postgres through the real start path (partitions exist, sink flushes). */
class SessionAuditWiringTest {

    @TempDir Path dir;

    @Test
    void presenceEventsAreWrittenToPostgres() throws Exception {
        try (PostgreSQLContainer pg = new PostgreSQLContainer("postgres:17-alpine")) {
            pg.start();
            Path tokens = dir.resolve("tokens");
            Files.writeString(tokens, "proxy-01=abc\n");
            AgoraService service = new AgoraService();
            NodeRegistry nodes = new NodeRegistry();
            NodeMessageDispatcher dispatcher = new NodeMessageDispatcher();
            PresenceService presence = new PresenceService(nodes);
            AgoraServer.wirePresence(service, presence, nodes, dispatcher);
            AgoraServer server = AgoraServer.start(new AgoraConfig(
                    0, pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), 4, tokens, Duration.ZERO),
                    service, new HolderRegistry(nodes), nodes, dispatcher, presence);
            try {
                NodeSession proxy = new NodeSession("proxy-01", NodeKind.VELOCITY, Instant.now(),
                        new StreamObserver<ServiceMessage>() {
                            public void onNext(ServiceMessage v) {}
                            public void onError(Throwable t) {}
                            public void onCompleted() {}
                        });
                nodes.register(proxy);
                UUID steve = UUID.randomUUID();
                assertThat(presence.claim(proxy, PresenceEntry.newBuilder().setPlayer(steve.toString())
                        .setName("Steve").build()).get(5, TimeUnit.SECONDS)).isTrue();
                presence.onPlayerSwitched(proxy,
                        PlayerSwitched.newBuilder().setPlayer(steve.toString()).setServer("lobby-01").build());
                presence.onPlayerLeft(proxy, PlayerLeft.newBuilder().setPlayer(steve.toString()).build());

                await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(actions(server, steve)).containsExactly("pantheon:join", "pantheon:quit"));
            } finally {
                server.close();
            }
        }
    }

    private static List<String> actions(AgoraServer server, UUID player) throws Exception {
        List<String> out = new ArrayList<>();
        try (Connection c = server.dataSource().getConnection();
                var ps = c.prepareStatement(
                        "SELECT action FROM pantheon_audit WHERE player = ? AND server = 'proxy-01' ORDER BY id")) {
            ps.setObject(1, player);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }
}

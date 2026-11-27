package de.kalypzo.agora;

import static org.assertj.core.api.Assertions.assertThat;

import de.kalypzo.agora.config.AgoraConfig;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.LocateRequest;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import de.kalypzo.agora.proto.v1.Hello;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

class AgoraServerTest {

    @TempDir Path dir;

    @Test
    void healthServingWithoutAuthAndRpcsWired() throws Exception {
        try (PostgreSQLContainer pg = new PostgreSQLContainer("postgres:17-alpine")) {
            pg.start();
            Path tokens = dir.resolve("tokens");
            Files.writeString(tokens, "n1=abc\n");
            AgoraServer server = AgoraServer.start(new AgoraConfig(
                    0, pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), 4, tokens, Duration.ZERO));
            ManagedChannel ch = NettyChannelBuilder.forAddress("localhost", server.port()).usePlaintext().build();
            try {
                assertThat(HealthGrpc.newBlockingStub(ch).check(HealthCheckRequest.getDefaultInstance()).getStatus())
                        .isEqualTo(ServingStatus.SERVING);
                Metadata md = new Metadata();
                md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), "n1");
                md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer abc");
                var stub = AgoraGrpc.newBlockingStub(ch).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
                // A-12: Locate is wired in (an unknown player is not found).
                assertThat(stub.locate(LocateRequest.newBuilder().setPlayer(java.util.UUID.randomUUID().toString())
                        .build()).getFound()).isFalse();
                // A-17: AppendAudit is wired in (an empty bundle is accepted, not UNIMPLEMENTED).
                stub.appendAudit(de.kalypzo.agora.proto.v1.AuditBundle.getDefaultInstance());
                // A-09: the Connect stream is wired in (paperWelcomeDelay 0 → Welcome at once).
                LinkedBlockingQueue<ServiceMessage> received = new LinkedBlockingQueue<>();
                StreamObserver<NodeMessage> conn = AgoraGrpc.newStub(ch)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
                        .connect(new StreamObserver<>() {
                            @Override public void onNext(ServiceMessage m) { received.add(m); }
                            @Override public void onError(Throwable t) {}
                            @Override public void onCompleted() {}
                        });
                conn.onNext(NodeMessage.newBuilder()
                        .setHello(Hello.newBuilder().setNodeId("n1").setKind(NodeKind.PAPER)).build());
                ServiceMessage welcome = received.poll(5, TimeUnit.SECONDS);
                assertThat(welcome).isNotNull();
                assertThat(welcome.hasWelcome()).isTrue();
                assertThat(server.nodes().get("n1")).isPresent();
                conn.onCompleted();
            } finally {
                ch.shutdownNow();
                server.close();
            }
        }
    }
}

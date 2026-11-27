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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

class AgoraServerTest {

    @TempDir Path dir;

    @Test
    void healthServingWithoutAuthAndRpcsUnimplemented() throws Exception {
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
                try {
                    stub.locate(LocateRequest.getDefaultInstance());
                    throw new AssertionError("expected UNIMPLEMENTED");
                } catch (StatusRuntimeException e) {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
                }
            } finally {
                ch.shutdownNow();
                server.close();
            }
        }
    }
}

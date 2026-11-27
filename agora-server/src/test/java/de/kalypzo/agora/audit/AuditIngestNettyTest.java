package de.kalypzo.agora.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import de.kalypzo.agora.AgoraServer;
import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.AuditBundle;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditValue;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-17 over the real Netty server: gzip on the wire and the 16 MiB inbound limit (E30). */
class AuditIngestNettyTest {

    @TempDir Path dir;

    PostgreSQLContainer pg = new PostgreSQLContainer("postgres:17-alpine");
    AgoraServer server;
    ManagedChannel ch;
    AgoraGrpc.AgoraBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        pg.start();
        Path tokens = dir.resolve("tokens");
        Files.writeString(tokens, "n1=abc\n");
        server = AgoraServer.start(new AgoraConfig(
                0, pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), 4, tokens, Duration.ZERO));
        ch = NettyChannelBuilder.forAddress("localhost", server.port()).usePlaintext().build();
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), "n1");
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer abc");
        stub = AgoraGrpc.newBlockingStub(ch).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }

    @AfterEach
    void stop() {
        ch.shutdownNow();
        server.close();
        pg.stop();
    }

    static ByteString bytes(UUID u) {
        return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(u.getMostSignificantBits())
                .putLong(u.getLeastSignificantBits()).array());
    }

    static AuditEntry.Builder entry() {
        return AuditEntry.newBuilder().setId(bytes(UuidV7.next(Clock.systemUTC())))
                .setPlayer(bytes(new UUID(0, 1))).setAction("shop:buy");
    }

    long count() throws Exception {
        try (var c = java.sql.DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
                var st = c.createStatement();
                var rs = st.executeQuery("SELECT count(*) FROM pantheon_audit WHERE server = 'n1'")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void gzipCompressedCallAccepted() throws Exception {
        stub.withCompression("gzip").appendAudit(AuditBundle.newBuilder().addEntries(entry()).build());
        assertThat(count()).isEqualTo(1);
    }

    @Test
    void fourMegabyteBundleAccepted() throws Exception {
        String text = "x".repeat(512);
        AuditBundle.Builder b = AuditBundle.newBuilder();
        for (int i = 0; i < 500; i++) {
            AuditEntry.Builder e = entry();
            for (int k = 0; k < 16; k++) e.putWith("key_" + k, AuditValue.newBuilder().setS(text).build());
            b.addEntries(e);
        }
        AuditBundle bundle = b.build();
        assertThat(bundle.getSerializedSize()).isGreaterThan(4 * 1024 * 1024);
        stub.appendAudit(bundle);
        assertThat(count()).isEqualTo(500);
    }
}

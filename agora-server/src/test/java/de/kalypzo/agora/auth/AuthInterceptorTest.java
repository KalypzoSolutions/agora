package de.kalypzo.agora.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.kalypzo.agora.proto.v1.AgoraGrpc;
import de.kalypzo.agora.proto.v1.LocateRequest;
import de.kalypzo.agora.proto.v1.LocateResponse;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

class AuthInterceptorTest {

    static final String TOKEN = "tok-Sup3rSecretValue";
    static final String WRONG = "tok-WrongValue99";

    @TempDir Path dir;
    Server server;
    ManagedChannel channel;
    final AtomicReference<NodeIdentity> seen = new AtomicReference<>();
    ListAppender<ILoggingEvent> appender;
    Logger root;

    @BeforeEach
    void setUp() throws Exception {
        Path file = dir.resolve("tokens");
        Files.writeString(file, "# comment\n\nvelocity-1=" + TOKEN + "\npaper-1=other-token\n");
        NodeTokens tokens = NodeTokens.load(file);
        AgoraGrpc.AgoraImplBase svc = new AgoraGrpc.AgoraImplBase() {
            @Override
            public void locate(LocateRequest request, StreamObserver<LocateResponse> out) {
                seen.set(NodeIdentity.CONTEXT_KEY.get());
                out.onNext(LocateResponse.getDefaultInstance());
                out.onCompleted();
            }
        };
        String name = "auth-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(svc)
                .intercept(new AuthInterceptor(tokens))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        appender = new ListAppender<>();
        appender.start();
        root.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        root.detachAppender(appender);
        channel.shutdownNow();
        server.shutdownNow();
    }

    private AgoraGrpc.AgoraBlockingStub stub(String node, String auth) {
        Metadata md = new Metadata();
        if (node != null) md.put(Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER), node);
        if (auth != null) md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), auth);
        Channel c = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md));
        return AgoraGrpc.newBlockingStub(c);
    }

    private void assertUnauthenticated(AgoraGrpc.AgoraBlockingStub s) {
        assertThatThrownBy(() -> s.locate(LocateRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void rejectsMissingToken() {
        assertUnauthenticated(stub("velocity-1", null));
        assertUnauthenticated(stub(null, "Bearer " + TOKEN));
    }

    @Test
    void rejectsWrongToken() {
        assertUnauthenticated(stub("velocity-1", "Bearer " + WRONG));
    }

    @Test
    void rejectsUnknownNode() {
        assertUnauthenticated(stub("ghost", "Bearer " + TOKEN));
    }

    @Test
    void acceptsValidTokenAndExposesNodeId() {
        stub("velocity-1", "Bearer " + TOKEN).locate(LocateRequest.getDefaultInstance());
        assertThat(seen.get()).isEqualTo(new NodeIdentity("velocity-1"));
    }

    @Test
    void tokenNeverLogged() {
        stub("velocity-1", "Bearer " + TOKEN).locate(LocateRequest.getDefaultInstance());
        assertUnauthenticated(stub("velocity-1", "Bearer " + WRONG));
        for (ILoggingEvent e : appender.list) {
            assertThat(e.getFormattedMessage()).doesNotContain(TOKEN).doesNotContain(WRONG);
        }
        assertThat(NodeTokens.load(dir.resolve("tokens")).toString()).doesNotContain(TOKEN);
    }
}

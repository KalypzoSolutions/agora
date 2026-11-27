package de.kalypzo.agora;

import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.auth.AuthInterceptor;
import de.kalypzo.agora.auth.NodeTokens;
import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.data.HolderSink;
import de.kalypzo.agora.data.ReadService;
import de.kalypzo.agora.db.Database;
import de.kalypzo.agora.grpc.DataEndpoints;
import de.kalypzo.agora.grpc.NodeStream;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceSnapshot;
import de.kalypzo.agora.index.IndexService;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Agora server: Postgres pool, migration, gRPC server with auth and health. */
public final class AgoraServer {

    private static final Logger LOG = LoggerFactory.getLogger(AgoraServer.class);

    private final AgoraConfig config;
    private final HikariDataSource dataSource;
    private final AgoraService service;
    private final HealthStatusManager health;
    private final Server server;
    private final IndexService indexes;
    private final NodeRegistry nodes;
    private final NodeMessageDispatcher nodeMessages;
    private final ScheduledExecutorService scheduler;

    private AgoraServer(
            AgoraConfig config,
            HikariDataSource dataSource,
            AgoraService service,
            HealthStatusManager health,
            Server server,
            IndexService indexes,
            NodeRegistry nodes,
            NodeMessageDispatcher nodeMessages,
            ScheduledExecutorService scheduler) {
        this.config = config;
        this.dataSource = dataSource;
        this.service = service;
        this.health = health;
        this.server = server;
        this.indexes = indexes;
        this.nodes = nodes;
        this.nodeMessages = nodeMessages;
        this.scheduler = scheduler;
    }

    /** Starts with an already built service (cards register their handlers on it first). */
    public static AgoraServer start(AgoraConfig config) {
        return start(config, new AgoraService());
    }

    public static AgoraServer start(AgoraConfig config, AgoraService service) {
        return start(config, service, HolderSink.NOOP);
    }

    /** {@code holders} receives the LoadPlayer claims (A-10 passes the holder registry; until then a no-op). */
    public static AgoraServer start(AgoraConfig config, AgoraService service, HolderSink holders) {
        return start(config, service, holders, new NodeRegistry(), new NodeMessageDispatcher(), PresenceSnapshot.EMPTY);
    }

    /**
     * Full wiring: {@code nodes} and {@code nodeMessages} can be built by the caller first, so that components that
     * need them (A-10 holders, A-11 presence) exist before the server starts; {@code presence} provides the
     * {@code Welcome} snapshot (A-11).
     */
    public static AgoraServer start(
            AgoraConfig config,
            AgoraService service,
            HolderSink holders,
            NodeRegistry nodes,
            NodeMessageDispatcher nodeMessages,
            PresenceSnapshot presence) {
        NodeTokens tokens = NodeTokens.load(config.tokensFile());
        HealthStatusManager health = new HealthStatusManager();
        health.setStatus("", ServingStatus.NOT_SERVING);
        HikariDataSource ds = Database.open(config);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "agora-scheduler");
            t.setDaemon(true);
            return t;
        });
        try {
            Migrations.migrate(ds);
            DataEndpoints.register(service, new ReadService(ds, holders));
            IndexService indexes = new IndexService(ds);
            DataEndpoints.registerIndexes(service, indexes);
            service.onConnect(new NodeStream(nodes, nodeMessages, presence, Clock.systemUTC(),
                    scheduler, config.paperWelcomeDelay(), NodeSession.DEFAULT_QUEUE_LIMIT));
            Server server = NettyServerBuilder.forPort(config.port())
                    .keepAliveTime(10, TimeUnit.SECONDS)
                    .permitKeepAliveTime(5, TimeUnit.SECONDS)
                    .permitKeepAliveWithoutCalls(true)
                    .intercept(new AuthInterceptor(tokens))
                    .addService(service)
                    .addService(health.getHealthService())
                    .build()
                    .start();
            health.setStatus("", ServingStatus.SERVING);
            health.setStatus("agora.v1.Agora", ServingStatus.SERVING);
            LOG.info("Agora listening on port {}", server.getPort());
            return new AgoraServer(config, ds, service, health, server, indexes, nodes, nodeMessages, scheduler);
        } catch (IOException e) {
            scheduler.shutdownNow();
            ds.close();
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            scheduler.shutdownNow();
            ds.close();
            throw e;
        }
    }

    public int port() {
        return server.getPort();
    }

    public AgoraConfig config() {
        return config;
    }

    public AgoraService service() {
        return service;
    }

    public HikariDataSource dataSource() {
        return dataSource;
    }

    /** The open node sessions (A-09). */
    public NodeRegistry nodes() {
        return nodes;
    }

    /** Routes every {@code NodeMessage} payload to registered listeners (A-09, R5). */
    public NodeMessageDispatcher nodeMessages() {
        return nodeMessages;
    }

    public Duration paperWelcomeDelay() {
        return config.paperWelcomeDelay();
    }

    public void close() {
        health.enterTerminalState();
        // R2: after NOT_SERVING no node-gone handling; nodes reconnect on UNAVAILABLE.
        nodes.shutdown(Status.UNAVAILABLE.withDescription("Agora is shutting down"));
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
                server.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            // Wait (bounded) for queued lane tasks before the pool they may use is closed.
            nodes.close(Duration.ofSeconds(5));
            scheduler.shutdownNow();
            indexes.close();
            dataSource.close();
        }
    }
}

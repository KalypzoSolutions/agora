package de.kalypzo.agora;

import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.auth.AuthInterceptor;
import de.kalypzo.agora.auth.NodeTokens;
import de.kalypzo.agora.config.AgoraConfig;
import de.kalypzo.agora.data.ChangePublisher;
import de.kalypzo.agora.data.HolderSink;
import de.kalypzo.agora.data.ReadService;
import de.kalypzo.agora.data.WriteService;
import de.kalypzo.agora.db.Database;
import de.kalypzo.agora.delivery.DeliveryRouter;
import de.kalypzo.agora.locate.LocateService;
import de.kalypzo.agora.grpc.DataEndpoints;
import de.kalypzo.agora.grpc.NodeStream;
import de.kalypzo.agora.grpc.PresenceEndpoints;
import de.kalypzo.agora.holder.HolderRegistry;
import de.kalypzo.agora.audit.AuditIngest;
import de.kalypzo.agora.audit.AuditPartitions;
import de.kalypzo.agora.audit.AuditSink;
import de.kalypzo.agora.audit.AuditStore;
import de.kalypzo.agora.audit.SessionAuditor;
import de.kalypzo.agora.audit.TimelineService;
import de.kalypzo.agora.grpc.AuditEndpoints;
import de.kalypzo.agora.jobs.AuditPartitionJob;
import de.kalypzo.agora.jobs.ExpireJob;
import de.kalypzo.agora.jobs.IdempotencyCleanupJob;
import de.kalypzo.agora.node.NodeMessageDispatcher;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceService;
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
    /** E30: a full audit bundle is about 4 MiB, the gRPC default. */
    static final int MAX_INBOUND_MESSAGE_SIZE = 16 * 1024 * 1024;

    private final AgoraService service;
    private final HealthStatusManager health;
    private final Server server;
    private final IndexService indexes;
    private final NodeRegistry nodes;
    private final NodeMessageDispatcher nodeMessages;
    private final ScheduledExecutorService scheduler;
    private final ScheduledExecutorService auditScheduler;
    private final AuditSink auditSink;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    private AgoraServer(
            AgoraConfig config,
            HikariDataSource dataSource,
            AgoraService service,
            HealthStatusManager health,
            Server server,
            IndexService indexes,
            NodeRegistry nodes,
            NodeMessageDispatcher nodeMessages,
            ScheduledExecutorService scheduler,
            ScheduledExecutorService auditScheduler,
            AuditSink auditSink) {
        this.config = config;
        this.dataSource = dataSource;
        this.service = service;
        this.health = health;
        this.server = server;
        this.indexes = indexes;
        this.nodes = nodes;
        this.nodeMessages = nodeMessages;
        this.scheduler = scheduler;
        this.auditScheduler = auditScheduler;
        this.auditSink = auditSink;
    }

    /**
     * The one programmatic entry (production wiring, also for in-process use from tests): data, indexes, holders and
     * change routing, presence, delivery, locate and the background jobs. {@code port = 0} picks a free port
     * ({@link #port()}), {@code paperWelcomeDelay = Duration.ZERO} sends {@code Welcome} to PAPER nodes at once.
     */
    public static AgoraServer start(AgoraConfig config) {
        return start(config, new AgoraService());
    }

    /**
     * Default wiring: presence (A-11) with delivery and locate (A-12, {@link #wirePresence}), and the holder
     * registry (A-10), which receives the LoadPlayer claims, the stream
     * messages and node-gone, and routes every committed change as {@code DocChanged} to the holders and watchers.
     */
    public static AgoraServer start(AgoraConfig config, AgoraService service) {
        NodeRegistry nodes = new NodeRegistry();
        NodeMessageDispatcher nodeMessages = new NodeMessageDispatcher();
        PresenceService presence = new PresenceService(nodes);
        wirePresence(service, presence, nodes, nodeMessages);
        HolderRegistry holders = new HolderRegistry(nodes);
        holders.install(nodeMessages);
        return start(config, service, holders, nodes, nodeMessages, presence, holders);
    }

    /**
     * {@code holders} additionally receives the LoadPlayer claims; a {@link HolderRegistry} of its own is installed
     * as well and routes the changes (A-10). Wires the presence (A-11): {@code ClaimPresence}, the stream handlers
     * and the {@code Welcome} snapshot, and delivery and locate (A-12, {@link #wirePresence}).
     */
    public static AgoraServer start(AgoraConfig config, AgoraService service, HolderSink holders) {
        NodeRegistry nodes = new NodeRegistry();
        NodeMessageDispatcher nodeMessages = new NodeMessageDispatcher();
        PresenceService presence = new PresenceService(nodes);
        wirePresence(service, presence, nodes, nodeMessages);
        return start(config, service, holders, nodes, nodeMessages, presence);
    }

    /**
     * Full wiring: {@code nodes} and {@code nodeMessages} can be built by the caller first, so that components that
     * need them (A-10 holders, A-11 presence) exist before the server starts; {@code presence} provides the
     * {@code Welcome} snapshot (A-11). This overload does not wire the presence handlers: a caller passing a
     * {@code PresenceService} also calls {@code presence.install(nodeMessages)} and
     * {@code PresenceEndpoints.register(service, presence, nodes)}, otherwise ClaimPresence stays UNIMPLEMENTED (see
     * {@link #start(AgoraConfig, AgoraService, HolderSink)}); {@link #wirePresence} does both plus delivery and locate
     * (A-12) in one call.
     *
     * <p>Changes are always routed (A-10): if {@code holders} is a {@link HolderRegistry} it is installed on
     * {@code nodeMessages} (idempotent) and used as publisher; otherwise a new {@code HolderRegistry} on {@code nodes}
     * is installed and {@code holders} additionally receives the LoadPlayer claims.
     */
    public static AgoraServer start(
            AgoraConfig config,
            AgoraService service,
            HolderSink holders,
            NodeRegistry nodes,
            NodeMessageDispatcher nodeMessages,
            PresenceSnapshot presence) {
        HolderRegistry registry = holders instanceof HolderRegistry r ? r : new HolderRegistry(nodes);
        registry.install(nodeMessages);
        HolderSink sink = registry == holders ? registry : (nodeId, player) -> {
            registry.addPlayerHolder(nodeId, player);
            holders.addPlayerHolder(nodeId, player);
        };
        return start(config, service, sink, nodes, nodeMessages, presence, registry);
    }

    /**
     * Wires everything that hangs off the presence (call before the server starts, once per service): the presence
     * handlers on {@code nodeMessages} and {@code ClaimPresence} (A-11), the {@link DeliveryRouter} for
     * {@code SendDelivery} and the {@link LocateService} for {@code Locate}/{@code LocationReply} (A-12, R5).
     */
    public static void wirePresence(
            AgoraService service, PresenceService presence, NodeRegistry nodes, NodeMessageDispatcher nodeMessages) {
        presence.install(nodeMessages);
        PresenceEndpoints.register(service, presence, nodes);
        new DeliveryRouter(nodes, presence).install(nodeMessages);
        LocateService locate = new LocateService(nodes, presence);
        locate.install(nodeMessages);
        locate.register(service);
    }

    /** As above; {@code changes} receives every committed write after its commit (A-10 routes it to the holders). */
    public static AgoraServer start(
            AgoraConfig config,
            AgoraService service,
            HolderSink holders,
            NodeRegistry nodes,
            NodeMessageDispatcher nodeMessages,
            PresenceSnapshot presence,
            ChangePublisher changes) {
        NodeTokens tokens = NodeTokens.load(config.tokensFile());
        HealthStatusManager health = new HealthStatusManager();
        health.setStatus("", ServingStatus.NOT_SERVING);
        HikariDataSource ds = Database.open(config);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "agora-scheduler");
            t.setDaemon(true);
            return t;
        });
        // E31: its own thread, so a slow Postgres write never delays the other jobs.
        ScheduledExecutorService auditScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "agora-audit-sink");
            t.setDaemon(true);
            return t;
        });
        AuditSink auditSink = null;
        try {
            Migrations.migrate(ds);
            // E39: the partitions exist before the server accepts AppendAudit and before health is SERVING.
            scheduleAuditPartitions(scheduler, ds);
            scheduleJobs(scheduler, ds, changes);
            DataEndpoints.register(service, new ReadService(ds, holders));
            DataEndpoints.registerWrites(service, new WriteService(ds, changes));
            IndexService indexes = new IndexService(ds);
            DataEndpoints.registerIndexes(service, indexes);
            AuditStore auditStore = new AuditStore(ds);
            auditSink = new AuditSink(auditStore, auditScheduler);
            // A-19: join/switch/quit from the real presence events (the partitions exist, E39).
            if (presence instanceof PresenceService ps) {
                ps.addEvents(new SessionAuditor(auditSink, Clock.systemUTC()));
            } else {
                LOG.warn("The presence is no PresenceService: no session audit entries");
            }
            AuditEndpoints.register(service, new AuditIngest(auditStore, Clock.systemUTC()));
            AuditEndpoints.timeline(service, new TimelineService(auditStore, ds, Clock.systemUTC()));
            service.onConnect(new NodeStream(nodes, nodeMessages, presence, Clock.systemUTC(),
                    scheduler, config.paperWelcomeDelay(), NodeSession.DEFAULT_QUEUE_LIMIT));
            Server server = NettyServerBuilder.forPort(config.port())
                    .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE) // E30
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
            return new AgoraServer(config, ds, service, health, server, indexes, nodes, nodeMessages, scheduler,
                    auditScheduler, auditSink);
        } catch (IOException e) {
            scheduler.shutdownNow();
            if (auditSink != null) auditSink.close();
            auditScheduler.shutdownNow();
            ds.close();
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            scheduler.shutdownNow();
            if (auditSink != null) auditSink.close();
            auditScheduler.shutdownNow();
            ds.close();
            throw e;
        }
    }

    /** Runs the audit partition job once now (a failure aborts the start) and then every 24 h (A-16, E39). */
    static void scheduleAuditPartitions(ScheduledExecutorService scheduler, javax.sql.DataSource ds) {
        AuditPartitionJob job = new AuditPartitionJob(new AuditPartitions(ds, Clock.systemUTC()));
        job.run();
        scheduleLogged(scheduler, job, AuditPartitionJob.INTERVAL, "audit partitions");
    }

    /** Background jobs on the scheduler (A-13) and the sweep of orphaned holder entries (A-10); failures are logged. */
    static void scheduleJobs(ScheduledExecutorService scheduler, javax.sql.DataSource ds, ChangePublisher changes) {
        Clock clock = Clock.systemUTC();
        scheduleLogged(scheduler, new ExpireJob(ds, changes, clock), ExpireJob.INTERVAL, "expire");
        scheduleLogged(scheduler, new IdempotencyCleanupJob(ds, clock), IdempotencyCleanupJob.INTERVAL,
                "idempotency cleanup");
        if (changes instanceof HolderRegistry registry) {
            scheduleLogged(scheduler, registry::sweepOrphans, HolderRegistry.GRACE, "holder sweep");
        }
    }

    private static void scheduleLogged(ScheduledExecutorService scheduler, Runnable job, Duration interval, String name) {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                job.run();
            } catch (RuntimeException e) {
                LOG.error("Background job '{}' failed", name, e);
            }
        }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
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

    /** Graceful shutdown (idempotent): NOT_SERVING, streams UNAVAILABLE, drain up to 10 s, close pool and jobs. */
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        health.enterTerminalState();
        // R2: after NOT_SERVING no node-gone handling; nodes reconnect on UNAVAILABLE.
        nodes.shutdown(Status.UNAVAILABLE.withDescription("Agora is shutting down"));
        server.shutdown();
        try {
            if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
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
            auditSink.close(); // flush the session entries before the pool closes
            auditScheduler.shutdownNow();
            indexes.close();
            dataSource.close();
        }
    }
}

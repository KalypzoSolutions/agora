package de.kalypzo.agora.grpc;

import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.data.ReadService;
import de.kalypzo.agora.data.UniqueViolationException;
import de.kalypzo.agora.data.WriteService;
import de.kalypzo.agora.index.IndexDeclarationException;
import de.kalypzo.agora.index.IndexService;
import de.kalypzo.agora.proto.v1.DeclareIndexResponse;
import de.kalypzo.agora.service.AgoraService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers the data RPCs (reads, writes, indexes) on the {@link AgoraService}. */
public final class DataEndpoints {

    private static final Logger LOG = LoggerFactory.getLogger(DataEndpoints.class);

    private DataEndpoints() {}

    @FunctionalInterface
    private interface Call<R> {
        R run() throws SQLException;
    }

    public static void register(AgoraService service, ReadService reads) {
        service.onLoadPlayer((req, out) -> reply(out, () -> {
            NodeIdentity caller = NodeIdentity.CONTEXT_KEY.get();
            if (caller == null) throw Status.UNAUTHENTICATED.withDescription("no node identity").asRuntimeException();
            return reads.loadPlayer(caller.nodeId(), req.getPlayer());
        }));
        service.onLoad((req, out) -> reply(out, () -> reads.load(req.getDoc())));
        service.onQuery((req, out) -> reply(out, () -> reads.query(req.getTarget(), req.getSpec())));
        service.onCount((req, out) -> reply(out, () -> reads.count(req)));
        service.onVersions((req, out) -> reply(out, () -> reads.versions(req)));
    }

    /**
     * Registers the write RPCs (A-07). A violated unique index answers {@code ALREADY_EXISTS} with the index name
     * (E27: Pantheon does not retry it); invalid requests, folded-amount overflows and Postgres data exceptions such
     * as a bigint overflow (22003) answer {@code INVALID_ARGUMENT}.
     */
    public static void registerWrites(AgoraService service, WriteService writes) {
        service.onApply((req, out) -> reply(out, () -> writes.apply(req)));
        service.onCreate((req, out) -> reply(out, () -> writes.create(req)));
        service.onDelete((req, out) -> reply(out, () -> writes.delete(req)));
        service.onExpire((req, out) -> reply(out, () -> writes.expire(req)));
        service.onTransfer((req, out) -> reply(out, () -> writes.transfer(req)));
    }

    /** Registers DeclareIndex (A-08). The build may outlast the client deadline; it then continues in the background. */
    public static void registerIndexes(AgoraService service, IndexService indexes) {
        service.onDeclareIndex((req, out) -> {
            CompletableFuture<String> future;
            try {
                future = indexes.declare(req.getIndex());
            } catch (IllegalArgumentException e) {
                out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
                return;
            }
            future.whenComplete((name, error) -> {
                if (error == null) {
                    out.onNext(DeclareIndexResponse.newBuilder().setName(name).build());
                    out.onCompleted();
                    return;
                }
                Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
                if (cause instanceof IndexDeclarationException e) {
                    Status status = e.duplicate() ? Status.ALREADY_EXISTS : Status.FAILED_PRECONDITION;
                    out.onError(status.withDescription(e.getMessage()).asRuntimeException());
                } else {
                    LOG.error("Index declaration failed", cause);
                    out.onError(Status.INTERNAL.withDescription("internal error").asRuntimeException());
                }
            });
        });
    }

    /** Runs the call and maps failures to gRPC statuses: bad input, unavailable database, internal error. */
    static <R> void reply(StreamObserver<R> out, Call<R> call) {
        try {
            R response = call.run();
            out.onNext(response);
            out.onCompleted();
        } catch (StatusRuntimeException e) {
            out.onError(e);
        } catch (UniqueViolationException e) {
            out.onError(Status.ALREADY_EXISTS.withDescription(e.index()).asRuntimeException());
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (SQLException e) {
            LOG.error("Database error", e);
            Status status = e instanceof SQLTransientException || "57014".equals(e.getSQLState())
                    ? Status.UNAVAILABLE
                    : Status.INTERNAL;
            out.onError(status.withDescription("database error").asRuntimeException());
        } catch (RuntimeException e) {
            LOG.error("Unexpected error", e);
            out.onError(Status.INTERNAL.withDescription("internal error").asRuntimeException());
        }
    }
}

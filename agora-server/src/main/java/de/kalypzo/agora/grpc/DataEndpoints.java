package de.kalypzo.agora.grpc;

import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.data.ReadService;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers the data RPCs on the {@link AgoraService}; the write RPCs follow with A-07. */
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

    /** Runs the call and maps failures to gRPC statuses: bad input, unavailable database, internal error. */
    static <R> void reply(StreamObserver<R> out, Call<R> call) {
        try {
            R response = call.run();
            out.onNext(response);
            out.onCompleted();
        } catch (StatusRuntimeException e) {
            out.onError(e);
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

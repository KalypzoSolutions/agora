package de.kalypzo.agora.jobs;

import de.kalypzo.agora.data.ChangePublisher;
import de.kalypzo.agora.proto.v1.DocChanged;
import de.kalypzo.agora.proto.v1.RowRef;
import de.kalypzo.agora.proto.v1.StoreDoc;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes expired store documents (agora §5 {@code expire}) and publishes each as {@code deleted} (version from the
 * sequence, no idempotency key, like the purge inside the writes, R41), so holders and watchers receive
 * {@code DocChanged(deleted)}. Runs every {@link #INTERVAL}; reads filter expired documents anyway.
 */
public final class ExpireJob implements Runnable {

    public static final Duration INTERVAL = Duration.ofSeconds(60);

    private static final int QUERY_TIMEOUT_SECONDS = 5;

    /** Rows deleted (and published) per transaction. */
    public static final int BATCH_SIZE = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(ExpireJob.class);

    private final DataSource ds;
    private final ChangePublisher publisher;
    private final Clock clock;
    private final int batchSize;

    public ExpireJob(DataSource ds, ChangePublisher publisher, Clock clock) {
        this(ds, publisher, clock, BATCH_SIZE);
    }

    ExpireJob(DataSource ds, ChangePublisher publisher, Clock clock, int batchSize) {
        this.ds = ds;
        this.publisher = publisher;
        this.clock = clock;
        this.batchSize = batchSize;
    }

    /** Deletes in batches of {@code batchSize} (commit + publish per batch) until a batch comes back short. */
    @Override
    public void run() {
        int deleted;
        do {
            List<DocChanged> changes = deleteBatch();
            deleted = changes.size();
            for (DocChanged change : changes) {
                try {
                    publisher.publish(change);
                } catch (RuntimeException e) {
                    LOG.error("Publishing the expiry of {} failed", change.getRef(), e);
                }
            }
        } while (deleted >= batchSize);
    }

    private List<DocChanged> deleteBatch() {
        List<DocChanged> changes = new ArrayList<>();
        // Rows are locked in (store, id) order, the lock order of the writes (no deadlock with Transfer/purge).
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM pantheon_doc d USING (SELECT store, id FROM pantheon_doc WHERE expires_at <= ?"
                                + " ORDER BY store COLLATE \"C\", id COLLATE \"C\" LIMIT ? FOR UPDATE) e"
                                + " WHERE d.store = e.store AND d.id = e.id"
                                + " RETURNING d.store, d.id, nextval('pantheon_version')")) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setTimestamp(1, Timestamp.from(clock.instant()));
            ps.setInt(2, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    RowRef ref = RowRef.newBuilder()
                            .setStoreDoc(StoreDoc.newBuilder().setStore(rs.getString(1)).setId(rs.getString(2)))
                            .build();
                    changes.add(DocChanged.newBuilder().setRef(ref).setVersion(rs.getLong(3)).setDeleted(true).build());
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("expire job failed", e);
        }
        return changes;
    }
}

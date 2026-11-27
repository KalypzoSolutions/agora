package de.kalypzo.agora.jobs;

import de.kalypzo.agora.audit.AuditPartitions;
import java.time.Duration;

/** Creates today and the next 2 days of audit partitions and drops those older than 14 days (E39); runs every 24 h. */
public final class AuditPartitionJob implements Runnable {

    public static final Duration INTERVAL = Duration.ofHours(24);
    public static final int DAYS_AHEAD = 2;
    public static final Duration RETENTION = Duration.ofDays(14);

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(AuditPartitionJob.class);

    private final AuditPartitions partitions;

    public AuditPartitionJob(AuditPartitions partitions) {
        this.partitions = partitions;
    }

    @Override
    public void run() {
        RuntimeException failure = null;
        try {
            partitions.ensure(DAYS_AHEAD);
        } catch (RuntimeException e) {
            LOG.error("Ensuring audit partitions failed", e);
            failure = e;
        }
        try {
            partitions.dropExpired(RETENTION);
        } catch (RuntimeException e) {
            LOG.error("Dropping expired audit partitions failed", e);
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        if (failure != null) throw failure;
    }
}

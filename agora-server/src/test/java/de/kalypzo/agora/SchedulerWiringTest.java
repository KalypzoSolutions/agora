package de.kalypzo.agora;

import static org.assertj.core.api.Assertions.assertThat;

import de.kalypzo.agora.data.ChangePublisher;
import de.kalypzo.agora.holder.HolderRegistry;
import de.kalypzo.agora.node.NodeRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** A-13: which background jobs scheduleJobs wires. */
class SchedulerWiringTest {

    static final class Recording extends ScheduledThreadPoolExecutor {
        final List<Runnable> tasks = new ArrayList<>();

        Recording() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable r, long initial, long delay, TimeUnit unit) {
            tasks.add(r);
            return null;
        }
    }

    @Test
    void sweepIsWiredOnlyForHolderRegistry() {
        Recording with = new Recording();
        Recording without = new Recording();
        try {
            AgoraServer.scheduleJobs(with, null, new HolderRegistry(new NodeRegistry()));
            AgoraServer.scheduleJobs(without, null, ChangePublisher.NOOP);
            assertThat(without.tasks).hasSize(2);
            assertThat(with.tasks).hasSize(3);
        } finally {
            with.shutdownNow();
            without.shutdownNow();
        }
    }
}

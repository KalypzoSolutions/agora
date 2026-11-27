package de.kalypzo.agora.audit;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/** UUIDv7 (48 bit Unix millis, version 7, variant 10); IDs from {@link #next} are monotonic within this JVM. */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int SEQ_MAX = 0xFFF;
    private static final Generator SHARED = new Generator();

    private UuidV7() {}

    /** Monotonic generator state; the static {@link #next} uses one shared instance, tests use their own. */
    static final class Generator {
        private final Object lock = new Object();
        private long lastMillis = -1;
        private int seq;

        UUID next(Clock clock) {
            long millis;
            int counter;
            synchronized (lock) {
                long now = clock.millis();
                if (now > lastMillis) {
                    lastMillis = now;
                    seq = RANDOM.nextInt(0x800); // leave headroom for increments
                } else if (seq < SEQ_MAX) {
                    seq++;
                } else {
                    lastMillis++;
                    seq = 0;
                }
                millis = lastMillis;
                counter = seq;
            }
            long msb = (millis << 16) | 0x7000L | counter;
            long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
            return new UUID(msb, lsb);
        }
    }

    /**
     * Next ID at the clock's time. Within one millisecond (or if the clock steps back) a 12-bit counter in
     * {@code rand_a} increases; on overflow the millisecond advances by one, so IDs are strictly ascending.
     */
    public static UUID next(Clock clock) {
        return SHARED.next(clock);
    }

    /** Smallest UUIDv7 of the instant's millisecond: all random bits 0. */
    public static UUID lowerBound(Instant t) {
        return new UUID((t.toEpochMilli() << 16) | 0x7000L, 0x8000000000000000L);
    }

    public static Instant timestamp(UUID id) {
        return Instant.ofEpochMilli(id.getMostSignificantBits() >>> 16);
    }
}

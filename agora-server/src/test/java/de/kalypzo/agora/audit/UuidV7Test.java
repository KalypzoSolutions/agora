package de.kalypzo.agora.audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** A-16: UUIDv7 utilities. */
class UuidV7Test {

    @Test
    void lowerBoundOfKnownDays() {
        assertThat(UuidV7.lowerBound(Instant.parse("2026-11-27T00:00:00Z")))
                .isEqualTo(UUID.fromString("01a21a4d-4000-7000-8000-000000000000"));
        assertThat(UuidV7.lowerBound(Instant.parse("2026-11-28T00:00:00Z")))
                .isEqualTo(UUID.fromString("01a21f73-9c00-7000-8000-000000000000"));
    }

    @Test
    void timestampRoundTrip() {
        Instant t = Instant.parse("2026-11-27T12:34:56.789Z");
        assertThat(UuidV7.timestamp(UuidV7.lowerBound(t))).isEqualTo(t);
        Clock clock = Clock.fixed(t, ZoneOffset.UTC);
        UUID id = UuidV7.next(clock);
        assertThat(UuidV7.timestamp(id)).isEqualTo(t);
        assertThat(id.version()).isEqualTo(7);
        assertThat(id.variant()).isEqualTo(2);
    }

    @Test
    void monotonicWithinMillisecond() {
        Clock clock = Clock.fixed(Instant.parse("2026-11-27T12:00:00Z"), ZoneOffset.UTC);
        byte[] previous = null;
        for (int i = 0; i < 1000; i++) {
            UUID id = UuidV7.next(clock);
            assertThat(id.version()).isEqualTo(7);
            assertThat(id.variant()).isEqualTo(2);
            byte[] bytes = ByteBuffer.allocate(16).putLong(id.getMostSignificantBits())
                    .putLong(id.getLeastSignificantBits()).array();
            if (previous != null) {
                assertThat(java.util.Arrays.compareUnsigned(previous, bytes)).isNegative();
            }
            previous = bytes;
        }
    }
}

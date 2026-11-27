package de.kalypzo.agora.node;

/** A registered callback; {@link #cancel()} removes it. Idempotent. */
@FunctionalInterface
public interface Subscription {
    void cancel();
}

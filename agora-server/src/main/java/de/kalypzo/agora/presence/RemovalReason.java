package de.kalypzo.agora.presence;

/** Why a presence entry was removed (audit_log.md §5: {@code reason} of {@code pantheon:quit}). */
public enum RemovalReason {
    /** The owning proxy reported {@code PlayerLeft}. */
    LEFT,
    /** A login over another proxy displaced the entry (double login). */
    REPLACED,
    /** The owning proxy's stream ended (node-gone). */
    PROXY_LOST
}

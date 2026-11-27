package de.kalypzo.agora.index;

/** An index could not be built. {@link #duplicate()} is true if a unique index failed on existing duplicates. */
public final class IndexDeclarationException extends RuntimeException {

    private final boolean duplicate;

    public IndexDeclarationException(String message, boolean duplicate) {
        super(message);
        this.duplicate = duplicate;
    }

    public boolean duplicate() {
        return duplicate;
    }
}

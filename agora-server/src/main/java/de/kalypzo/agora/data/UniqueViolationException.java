package de.kalypzo.agora.data;

/** A write violated a declared unique index; the endpoints answer {@code ALREADY_EXISTS} with the index name. */
public final class UniqueViolationException extends RuntimeException {

    private final String index;

    public UniqueViolationException(String index) {
        super(index);
        this.index = index;
    }

    public String index() {
        return index;
    }
}

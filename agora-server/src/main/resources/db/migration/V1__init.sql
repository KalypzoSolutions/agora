CREATE SEQUENCE pantheon_version;

-- Spielerdaten: eine Zeile pro Spieler und Namespace
CREATE TABLE pantheon_player (
    player     UUID        NOT NULL,
    namespace  TEXT        NOT NULL,
    data       JSONB       NOT NULL DEFAULT '{}',
    version    BIGINT      NOT NULL DEFAULT nextval('pantheon_version'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player, namespace)
);

-- Stores: eine Zeile pro Dokument
CREATE TABLE pantheon_doc (
    store      TEXT        NOT NULL,              -- '<namespace>:<store>', z. B. 'lobby:clans'
    id         TEXT        NOT NULL,
    data       JSONB       NOT NULL DEFAULT '{}',
    version    BIGINT      NOT NULL DEFAULT nextval('pantheon_version'),
    expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store, id)
);

-- Bereits ausgeführte Schreiboperationen
CREATE TABLE pantheon_idempotency (
    idempotency_key TEXT        PRIMARY KEY,
    result          JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Deklarierte Indizes
CREATE TABLE pantheon_index (
    name       TEXT        PRIMARY KEY,           -- z. B. 'pantheon_player_economy_coins'
    definition TEXT        NOT NULL,
    status     TEXT        NOT NULL,              -- 'pending', 'ready', 'failed'
    error      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX pantheon_idempotency_created_at ON pantheon_idempotency (created_at);
CREATE INDEX pantheon_doc_expires_at ON pantheon_doc (expires_at) WHERE expires_at IS NOT NULL;

-- Menge in einem JSONB-Array: fehlt das Array, wird es angelegt; das Element kommt nur hinzu, wenn es fehlt.
CREATE FUNCTION pantheon_set_add(data jsonb, key text, elem jsonb) RETURNS jsonb
    LANGUAGE sql IMMUTABLE AS
$$
SELECT CASE
    WHEN jsonb_typeof(data -> key) IS DISTINCT FROM 'array'
        THEN jsonb_set(data, ARRAY[key], jsonb_build_array(elem))
    WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(data -> key) AS e(v) WHERE e.v = elem)
        THEN data
    ELSE jsonb_set(data, ARRAY[key], (data -> key) || jsonb_build_array(elem))
END
$$;

-- Entfernt alle Vorkommen des Elements; fehlt das Array, bleibt data unverändert.
CREATE FUNCTION pantheon_set_remove(data jsonb, key text, elem jsonb) RETURNS jsonb
    LANGUAGE sql IMMUTABLE AS
$$
SELECT CASE
    WHEN jsonb_typeof(data -> key) IS DISTINCT FROM 'array' THEN data
    ELSE jsonb_set(data, ARRAY[key], COALESCE(
        (SELECT jsonb_agg(e.v ORDER BY e.n)
           FROM jsonb_array_elements(data -> key) WITH ORDINALITY AS e(v, n)
          WHERE e.v <> elem),
        '[]'::jsonb))
END
$$;

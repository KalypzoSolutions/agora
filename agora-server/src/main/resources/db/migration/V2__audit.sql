-- Audit-Log (audit_log.md §9): nach UUIDv7-id partitioniert, eine Partition pro Tag.
-- Partitionen legt Agora beim Start und danach täglich an (AuditPartitions), nicht Flyway.
CREATE TABLE pantheon_audit (
    id         UUID             NOT NULL,          -- UUIDv7, enthält den Zeitpunkt
    player     UUID             NOT NULL,
    target     UUID,
    trace      UUID,
    server     TEXT             NOT NULL,
    action     TEXT             NOT NULL,
    world      TEXT,
    x          DOUBLE PRECISION,
    y          DOUBLE PRECISION,
    z          DOUBLE PRECISION,
    with_data  JSONB            NOT NULL DEFAULT '{}',
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE INDEX pantheon_audit_player ON pantheon_audit (player, id DESC);
CREATE INDEX pantheon_audit_target ON pantheon_audit (target, id DESC) WHERE target IS NOT NULL;

package de.kalypzo.agora.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class SortSqlTest {

    @Test
    void textTieBreakIsBinary() {
        assertThat(SortSql.orderBy(f -> "d", List.of(), "d.id", true).sql()).isEqualTo("ORDER BY d.id COLLATE \"C\" ASC");
        assertThat(SortSql.orderBy("p0", List.of(), "p0.player").sql()).isEqualTo("ORDER BY p0.player ASC");
    }
}

package de.kalypzo.agora.sql;

import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.Sort;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** {@code ORDER BY} clause: each sort {@code NULLS LAST}, always ending with an ascending tie-break on the ID column. */
public final class SortSql {

    private SortSql() {}

    public static SqlFragment orderBy(String alias, List<Sort> sorts, String tieBreakColumn) {
        return orderBy(f -> alias, sorts, tieBreakColumn);
    }

    /** Like {@link #orderBy(String, List, String)}, with the row alias chosen per field (e.g. one join per namespace). */
    public static SqlFragment orderBy(Function<FieldSpec, String> aliasFor, List<Sort> sorts, String tieBreakColumn) {
        String tieBreak = SqlLiteral.column(tieBreakColumn);
        List<SqlFragment> keys = new ArrayList<>();
        for (Sort s : sorts) {
            FieldSpec f = s.getField();
            if (Values.isJsonType(f.getType())) {
                throw new IllegalArgumentException("cannot sort by " + f.getType() + " field " + f.getName());
            }
            keys.add(SqlFragment.concat(
                    FieldExpr.read(aliasFor.apply(f), f),
                    SqlFragment.of(s.getDescending() ? " DESC NULLS LAST" : " ASC NULLS LAST")));
        }
        keys.add(SqlFragment.of(tieBreak + " ASC"));
        return SqlFragment.concat(SqlFragment.of("ORDER BY "), SqlFragment.join(", ", keys));
    }
}

package de.kalypzo.agora.sql;

import de.kalypzo.agora.proto.v1.Condition;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.Predicate;
import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.Value;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL for a {@link Condition} (E14): comparisons with {@code NULL} are {@code false}, {@code not(p)} is
 * {@code NOT COALESCE((p), false)}, strings compare binary. Values are always bound parameters.
 *
 * <p>The result is parenthesized and may evaluate to {@code NULL}, which {@code WHERE} treats as {@code false}.
 */
public final class PredicateSql {

    private PredicateSql() {}

    public static SqlFragment where(String alias, Condition c) {
        return predicate(alias, c.getField(), c.getPredicate());
    }

    private static SqlFragment predicate(String alias, FieldSpec f, Predicate p) {
        return switch (p.getKindCase()) {
            case EQ -> compare(alias, f, "=", p.getEq());
            case NE -> compare(alias, f, "<>", p.getNe());
            case GT -> compare(alias, f, ">", p.getGt());
            case GTE -> compare(alias, f, ">=", p.getGte());
            case LT -> compare(alias, f, "<", p.getLt());
            case LTE -> compare(alias, f, "<=", p.getLte());
            case CONTAINS -> contains(alias, f, p.getContains());
            case IN_VALUES -> in(alias, f, p.getInValues().getValuesList());
            case EXISTS -> {
                // Ignores the default. Equivalent to `data ? 'k'`, without a `?` that JDBC would take for a parameter.
                String name = SqlLiteral.fieldName(f.getName());
                String data = SqlLiteral.dataColumn(alias);
                yield SqlFragment.of(
                        "((" + data + "->'" + name + "') IS " + (p.getExists() ? "NOT NULL)" : "NULL)"));
            }
            case NEGATE -> SqlFragment.concat(
                    SqlFragment.of("(NOT COALESCE("),
                    predicate(alias, f, p.getNegate()),
                    SqlFragment.of(", false))"));
            case KIND_NOT_SET -> throw new IllegalArgumentException("predicate without kind on " + f.getName());
        };
    }

    private static SqlFragment compare(String alias, FieldSpec f, String op, Value v) {
        SqlFragment read = FieldExpr.read(alias, f);
        boolean json = Values.isJsonType(f.getType());
        if (json && !op.equals("=") && !op.equals("<>")) {
            throw new IllegalArgumentException("only eq/ne on " + f.getType() + " field " + f.getName());
        }
        Values.requireCompatible(v, f.getType(), "comparison value");
        if (Values.isNull(v)) return SqlFragment.of("(false)");
        return SqlFragment.concat(
                SqlFragment.of("(("), read, SqlFragment.of(") " + op + " "), param(f.getType(), v), SqlFragment.of(")"));
    }

    private static SqlFragment contains(String alias, FieldSpec f, Value elem) {
        if (f.getType() != ScalarType.LIST && f.getType() != ScalarType.SET) {
            throw new IllegalArgumentException("contains needs a LIST or SET field: " + f.getName());
        }
        if (elem.getKindCase() == Value.KindCase.KIND_NOT_SET) {
            throw new IllegalArgumentException("contains without element on " + f.getName());
        }
        return SqlFragment.concat(
                SqlFragment.of("(("),
                FieldExpr.read(alias, f),
                SqlFragment.of(") @> jsonb_build_array(?::jsonb))", Values.toJson(elem)));
    }

    private static SqlFragment in(String alias, FieldSpec f, List<Value> values) {
        if (values.isEmpty()) {
            SqlLiteral.fieldName(f.getName());
            return SqlFragment.of("(false)");
        }
        List<SqlFragment> items = new ArrayList<>();
        for (Value v : values) {
            Values.requireCompatible(v, f.getType(), "in value");
            items.add(Values.isNull(v) ? SqlFragment.of("NULL::" + SqlLiteral.sqlType(f.getType())) : param(f.getType(), v));
        }
        return SqlFragment.concat(
                SqlFragment.of("(("),
                FieldExpr.read(alias, f),
                SqlFragment.of(") IN ("),
                SqlFragment.join(", ", items),
                SqlFragment.of("))"));
    }

    /** A bound, typed parameter for a non-null value. */
    private static SqlFragment param(ScalarType t, Value v) {
        if (Values.isJsonType(t)) return SqlFragment.of("?::jsonb", Values.toJson(v));
        return SqlFragment.of("?::" + SqlLiteral.sqlType(t), Values.scalarParam(v));
    }
}

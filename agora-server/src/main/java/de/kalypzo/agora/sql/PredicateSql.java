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
 * <p>The result is parenthesized and always {@code true} or {@code false}, never {@code NULL} (R35). Subqueries use
 * the aliases {@code pantheon_l}, {@code pantheon_r} and {@code pantheon_e}, which callers must not use.
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
        if (f.getType() == ScalarType.SET) return setEquals(read, v, op.equals("="));
        return notNullAnd(read, f, SqlFragment.concat(
                SqlFragment.of("("), read, SqlFragment.of(") " + op + " "), param(f.getType(), v)));
    }

    /**
     * R33: SET equality ignores order; elements compare with jsonb equality (deep). A missing value (no array) is
     * neither equal nor unequal.
     */
    private static SqlFragment setEquals(SqlFragment read, Value v, boolean equal) {
        String r = read.sql(); // reads carry no parameters (R4)
        String json = Values.toJson(v);
        String body = "NOT EXISTS (SELECT 1 FROM jsonb_array_elements(" + r + ") pantheon_l(v)"
                + " WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(?::jsonb) pantheon_r(v)"
                + " WHERE pantheon_r.v = pantheon_l.v))"
                + " AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(?::jsonb) pantheon_r(v)"
                + " WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(" + r + ") pantheon_l(v)"
                + " WHERE pantheon_l.v = pantheon_r.v))";
        return new SqlFragment(
                "(CASE WHEN jsonb_typeof(" + r + ") = 'array' THEN " + (equal ? "(" + body + ")" : "NOT (" + body + ")")
                        + " ELSE false END)",
                List.of(json, json));
    }

    /** R32: an element of the array equals the value (jsonb equality, deep), not jsonb containment. */
    private static SqlFragment contains(String alias, FieldSpec f, Value elem) {
        if (f.getType() != ScalarType.LIST && f.getType() != ScalarType.SET) {
            throw new IllegalArgumentException("contains needs a LIST or SET field: " + f.getName());
        }
        if (elem.getKindCase() == Value.KindCase.KIND_NOT_SET) {
            throw new IllegalArgumentException("contains without element on " + f.getName());
        }
        String r = FieldExpr.read(alias, f).sql();
        return SqlFragment.of(
                "(CASE WHEN jsonb_typeof(" + r + ") = 'array' THEN EXISTS (SELECT 1 FROM jsonb_array_elements(" + r
                        + ") pantheon_e(v) WHERE pantheon_e.v = ?::jsonb) ELSE false END)",
                Values.toJson(elem));
    }

    private static SqlFragment in(String alias, FieldSpec f, List<Value> values) {
        SqlFragment read = FieldExpr.read(alias, f);
        List<SqlFragment> items = new ArrayList<>();
        for (Value v : values) {
            Values.requireCompatible(v, f.getType(), "in value");
            if (!Values.isNull(v)) items.add(param(f.getType(), v)); // NULL never matches (E14)
        }
        if (items.isEmpty()) return SqlFragment.of("(false)");
        if (f.getType() == ScalarType.SET) { // R37: OR of order-independent set equalities
            List<SqlFragment> eqs = new ArrayList<>();
            for (Value v : values) if (!Values.isNull(v)) eqs.add(setEquals(read, v, true));
            return SqlFragment.concat(SqlFragment.of("("), SqlFragment.join(" OR ", eqs), SqlFragment.of(")"));
        }
        return notNullAnd(read, f, SqlFragment.concat(
                SqlFragment.of("("), read, SqlFragment.of(") IN ("), SqlFragment.join(", ", items), SqlFragment.of(")")));
    }

    /**
     * R35: {@code ((read) IS NOT NULL AND expr)} so the result is never SQL NULL, given that {@code expr} only yields
     * NULL for a NULL read. Fields with a non-null default can't read as NULL and keep the bare {@code (expr)}. The
     * comparison stays a top-level conjunct, so expression indexes still apply.
     */
    private static SqlFragment notNullAnd(SqlFragment read, FieldSpec f, SqlFragment expr) {
        if (f.hasDefaultValue() && !Values.isNull(f.getDefaultValue())) {
            return SqlFragment.concat(SqlFragment.of("("), expr, SqlFragment.of(")"));
        }
        return SqlFragment.concat(
                SqlFragment.of("(("), read, SqlFragment.of(") IS NOT NULL AND "), expr, SqlFragment.of(")"));
    }

    /** A bound, typed parameter for a non-null value. */
    private static SqlFragment param(ScalarType t, Value v) {
        if (Values.isJsonType(t)) return SqlFragment.of("?::jsonb", Values.toJson(v));
        return SqlFragment.of("?::" + SqlLiteral.sqlType(t), Values.scalarParam(v));
    }
}

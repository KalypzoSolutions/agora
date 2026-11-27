package de.kalypzo.agora.sql;

import de.kalypzo.agora.proto.v1.Condition;
import de.kalypzo.agora.proto.v1.FieldOp;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.Predicate;
import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the {@code SET data = …} expression for the operations on one row (agora.md §7, E14).
 *
 * <p>Each touched field gets one jsonb value expression that chains its operations in call order, starting from the
 * stored value ({@code data->'k'}); SQL {@code NULL} means "absent". The fields are then written with nested
 * {@code jsonb_set_lax(…, 'delete_key')}, so a {@code NULL} result removes the key ({@code set(null)} ≡ {@code unset}).
 * Every sub-expression appears exactly once, so the SQL grows linearly with the number of operations.
 */
public final class UpdateBuilder {

    private UpdateBuilder() {}

    public static SqlFragment dataExpression(List<FieldOp> opsForOneRow) {
        Map<String, SqlFragment> values = new LinkedHashMap<>();
        for (FieldOp op : opsForOneRow) {
            String name = SqlLiteral.fieldName(op.getField().getName());
            SqlFragment current = values.getOrDefault(name, SqlFragment.of("(data->'" + name + "')"));
            values.put(name, apply(current, op));
        }
        SqlFragment expr = SqlFragment.of("data");
        for (Map.Entry<String, SqlFragment> e : values.entrySet()) {
            expr = SqlFragment.concat(
                    SqlFragment.of("jsonb_set_lax("),
                    expr,
                    SqlFragment.of(", '{" + e.getKey() + "}', "),
                    e.getValue(),
                    SqlFragment.of(", true, 'delete_key')"));
        }
        return expr;
    }

    /**
     * The conditions {@code withdraw} needs: the value before the withdraw must be at least the amount. Preceding
     * {@code inc}/{@code withdraw} on the same field are folded in, so all conditions refer to the stored row. A
     * {@code withdraw} after any other operation on the same field is rejected.
     */
    public static List<Condition> implicitConditions(List<FieldOp> ops) {
        try {
            return conditions(ops);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("inc/withdraw amounts overflow a long", e);
        }
    }

    private static List<Condition> conditions(List<FieldOp> ops) {
        List<Condition> out = new ArrayList<>();
        Map<String, Long> delta = new HashMap<>();
        Map<String, Boolean> tainted = new HashMap<>();
        for (FieldOp op : ops) {
            String name = SqlLiteral.fieldName(op.getField().getName());
            switch (op.getOpCase()) {
                case INC -> delta.merge(name, op.getInc(), Math::addExact);
                case WITHDRAW -> {
                    long amount = requirePositive(op.getWithdraw());
                    if (tainted.getOrDefault(name, false)) {
                        throw new IllegalArgumentException(
                                "withdraw after a non-additive operation on field " + name);
                    }
                    long before = delta.getOrDefault(name, 0L);
                    long required = Math.subtractExact(amount, before);
                    FieldSpec f = op.getField();
                    // R38: a missing field without default counts as 0
                    if (!f.hasDefaultValue() || Values.isNull(f.getDefaultValue())) {
                        f = f.toBuilder().setDefaultValue(Value.newBuilder().setLongValue(0)).build();
                    }
                    out.add(Condition.newBuilder()
                            .setField(f)
                            .setPredicate(Predicate.newBuilder()
                                    .setGte(Value.newBuilder().setLongValue(required)))
                            .build());
                    delta.put(name, Math.subtractExact(before, amount));
                }
                default -> tainted.put(name, true);
            }
        }
        return out;
    }

    private static SqlFragment apply(SqlFragment current, FieldOp op) {
        FieldSpec f = op.getField();
        ScalarType t = f.getType();
        return switch (op.getOpCase()) {
            case SET -> {
                Values.requireCompatible(op.getSet(), t, "set value");
                yield Values.isNull(op.getSet())
                        ? SqlFragment.of("NULL::jsonb")
                        : SqlFragment.of("?::jsonb", Values.toJson(op.getSet()));
            }
            case UNSET -> SqlFragment.of("NULL::jsonb");
            case INC -> add(current, f, op.getInc());
            case WITHDRAW -> add(current, f, -requirePositive(op.getWithdraw()));
            case MAX -> extreme("GREATEST", current, f, op.getMax());
            case MIN -> extreme("LEAST", current, f, op.getMin());
            case ADD -> {
                Value elem = requireElement(f, op.getAdd());
                if (t == ScalarType.LIST) {
                    yield SqlFragment.concat(
                            SqlFragment.of("(COALESCE("),
                            orDefault(current, f),
                            SqlFragment.of(", '[]'::jsonb) || jsonb_build_array(?::jsonb))", Values.toJson(elem)));
                }
                yield SqlFragment.concat(
                        SqlFragment.of("(pantheon_set_add(jsonb_build_object('v', "),
                        orDefault(current, f),
                        SqlFragment.of("), 'v', ?::jsonb) -> 'v')", Values.toJson(elem)));
            }
            case REMOVE -> {
                Value elem = requireElement(f, op.getRemove());
                yield SqlFragment.concat(
                        SqlFragment.of("NULLIF(pantheon_set_remove(jsonb_build_object('v', "),
                        orDefault(current, f),
                        SqlFragment.of("), 'v', ?::jsonb) -> 'v', 'null'::jsonb)", Values.toJson(elem)));
            }
            case OP_NOT_SET -> throw new IllegalArgumentException("operation without kind on field " + f.getName());
        };
    }

    /** {@code to_jsonb(COALESCE(<current as number>, d or 0) + delta)}; {@code withdraw} is {@code inc(-amount)}. */
    private static SqlFragment add(SqlFragment current, FieldSpec f, long delta) {
        if (f.getType() != ScalarType.LONG && f.getType() != ScalarType.DOUBLE) {
            throw new IllegalArgumentException("inc/withdraw needs a LONG or DOUBLE field: " + f.getName());
        }
        SqlFragment value = typedOf(current, f);
        if (!f.hasDefaultValue() || Values.isNull(f.getDefaultValue())) {
            // R34: without a default a missing number starts at 0.
            value = SqlFragment.concat(SqlFragment.of("COALESCE("), value, SqlFragment.of(", 0)"));
        }
        return SqlFragment.concat(
                SqlFragment.of("to_jsonb("),
                value,
                SqlFragment.of(" + ?::" + SqlLiteral.sqlType(f.getType()) + ")", delta));
    }

    private static SqlFragment extreme(String fn, SqlFragment current, FieldSpec f, Value v) {
        ScalarType t = f.getType();
        if (t != ScalarType.LONG && t != ScalarType.DOUBLE && t != ScalarType.STRING) {
            throw new IllegalArgumentException("max/min needs a LONG, DOUBLE or STRING field: " + f.getName());
        }
        Values.requireCompatible(v, t, "max/min value");
        if (Values.isNull(v)) throw new IllegalArgumentException("max/min value must not be null");
        return SqlFragment.concat(
                SqlFragment.of("to_jsonb(" + fn + "("),
                typedOf(current, f),
                SqlFragment.of(", ?::" + SqlLiteral.sqlType(t) + "))", Values.scalarParam(v)));
    }

    /** The jsonb value {@code current} as the field's SQL type, with default. */
    private static SqlFragment typedOf(SqlFragment current, FieldSpec f) {
        return FieldExpr.typed(SqlFragment.concat(SqlFragment.of("("), current, SqlFragment.of(" #>> '{}')")), f);
    }

    /** {@code COALESCE(current, d)} for a jsonb field, or {@code current} without default. */
    private static SqlFragment orDefault(SqlFragment current, FieldSpec f) {
        return FieldExpr.withDefault(current, f);
    }

    private static Value requireElement(FieldSpec f, Value elem) {
        if (f.getType() != ScalarType.LIST && f.getType() != ScalarType.SET) {
            throw new IllegalArgumentException("add/remove needs a LIST or SET field: " + f.getName());
        }
        if (elem.getKindCase() == Value.KindCase.KIND_NOT_SET || Values.isNull(elem)) {
            throw new IllegalArgumentException("add/remove element must not be null");
        }
        return elem;
    }

    private static long requirePositive(long amount) {
        if (amount <= 0) throw new IllegalArgumentException("withdraw amount must be > 0, was " + amount);
        return amount;
    }
}

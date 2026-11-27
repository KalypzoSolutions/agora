package de.kalypzo.agora.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.db.Migrations;
import de.kalypzo.agora.proto.v1.Condition;
import de.kalypzo.agora.proto.v1.FieldOp;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.Predicate;
import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.Sort;
import de.kalypzo.agora.proto.v1.Value;
import de.kalypzo.agora.proto.v1.ValueList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Semantics of the SQL building blocks against a real Postgres (E14, agora.md §7). */
class SqlSemanticsTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;
    static final AtomicInteger ids = new AtomicInteger();

    @BeforeAll
    static void start() {
        postgres.start();
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(postgres.getJdbcUrl());
        cfg.setUsername(postgres.getUsername());
        cfg.setPassword(postgres.getPassword());
        cfg.setMaximumPoolSize(4);
        ds = new HikariDataSource(cfg);
        Migrations.migrate(ds);
    }

    @AfterAll
    static void stop() {
        ds.close();
        postgres.stop();
    }

    // ---------- the card's cases ----------

    @Test
    void incUsesDefaultWhenMissing() throws SQLException {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(1));
        assertThat(read(apply("{}", inc(coins, 2)), coins)).isEqualTo("3");
    }

    @Test
    void incKeepsLongExact() throws SQLException {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(0));
        String data = apply("{\"coins\": " + (Long.MAX_VALUE - 1) + "}", inc(coins, 1));
        assertThat(read(data, coins)).isEqualTo(Long.toString(Long.MAX_VALUE));
        assertThat(data).isEqualTo("{\"coins\": 9223372036854775807}");
    }

    @Test
    void withdrawConditionFailsBelowAmount() throws SQLException {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(0));
        assertThat(apply("{\"coins\": 50}", withdraw(coins, 60))).isNull();
        assertThat(read(apply("{\"coins\": 50}", withdraw(coins, 50)), coins)).isEqualTo("0");
        assertThat(apply("{}", withdraw(coins, 1))).isNull();
    }

    @Test
    void maxOnlyRaises() throws SQLException {
        FieldSpec level = field("level", ScalarType.LONG, lv(1));
        assertThat(read(apply("{\"level\": 5}", max(level, lv(3))), level)).isEqualTo("5");
        assertThat(read(apply("{\"level\": 5}", max(level, lv(7))), level)).isEqualTo("7");
    }

    @Test
    void minUsesValueWhenNoDefault() throws SQLException {
        FieldSpec best = field("best", ScalarType.LONG);
        assertThat(apply("{}", min(best, lv(4)))).isEqualTo("{\"best\": 4}");
    }

    @Test
    void setAddIgnoresDuplicate() throws SQLException {
        FieldSpec kits = field("kits", ScalarType.SET, list());
        assertThat(apply("{\"kits\": [\"a\"]}", add(kits, sv("a")))).isEqualTo("{\"kits\": [\"a\"]}");
        assertThat(apply("{}", add(kits, sv("a")))).isEqualTo("{\"kits\": [\"a\"]}");
    }

    @Test
    void listAddAppendsDuplicate() throws SQLException {
        FieldSpec log = field("log", ScalarType.LIST, list());
        assertThat(apply("{\"log\": [\"a\"]}", add(log, sv("a")))).isEqualTo("{\"log\": [\"a\", \"a\"]}");
    }

    @Test
    void removeRemovesAll() throws SQLException {
        FieldSpec log = field("log", ScalarType.LIST, list());
        assertThat(apply("{\"log\": [\"a\", \"b\", \"a\"]}", remove(log, sv("a"))))
                .isEqualTo("{\"log\": [\"b\"]}");
        FieldSpec noDefault = field("log", ScalarType.LIST);
        assertThat(apply("{}", remove(noDefault, sv("a")))).isEqualTo("{}");
    }

    @Test
    void setNullUnsets() throws SQLException {
        FieldSpec home = field("home", ScalarType.STRING);
        FieldOp setNull = FieldOp.newBuilder()
                .setField(home)
                .setSet(Value.newBuilder().setNullValue(true))
                .build();
        assertThat(apply("{\"home\": \"x\", \"k\": 1}", setNull)).isEqualTo("{\"k\": 1}");
        FieldOp unset = FieldOp.newBuilder().setField(home).setUnset(true).build();
        assertThat(apply("{\"home\": \"x\", \"k\": 1}", unset)).isEqualTo("{\"k\": 1}");
    }

    @Test
    void neIsFalseOnNull() throws SQLException {
        FieldSpec level = field("level", ScalarType.LONG);
        assertThat(test("{}", cond(level, Predicate.newBuilder().setNe(lv(5)).build()))).isFalse();
        assertThat(test("{\"level\": 4}", cond(level, Predicate.newBuilder().setNe(lv(5)).build())))
                .isTrue();
    }

    @Test
    void notIsTrueOnNull() throws SQLException {
        FieldSpec level = field("level", ScalarType.LONG);
        Predicate eq5 = Predicate.newBuilder().setEq(lv(5)).build();
        Predicate not = Predicate.newBuilder().setNegate(eq5).build();
        assertThat(test("{}", cond(level, eq5))).isFalse();
        assertThat(test("{}", cond(level, not))).isTrue();
        assertThat(test("{\"level\": 5}", cond(level, not))).isFalse();
    }

    @Test
    void containsUsesDefaultEmptyArray() throws SQLException {
        FieldSpec kits = field("kits", ScalarType.SET, list());
        Predicate containsA = Predicate.newBuilder().setContains(sv("a")).build();
        assertThat(read("{}", kits)).isEqualTo("[]");
        assertThat(test("{}", cond(kits, containsA))).isFalse();
        assertThat(test("{}", cond(kits, Predicate.newBuilder().setNegate(containsA).build())))
                .isTrue();
        FieldSpec withA = field("kits", ScalarType.SET, list(sv("a")));
        assertThat(test("{}", cond(withA, containsA))).isTrue();
        assertThat(test("{\"kits\": [\"b\"]}", cond(withA, containsA))).isFalse();
    }

    @Test
    void stringCompareIsBinary() throws SQLException {
        FieldSpec name = field("name", ScalarType.STRING);
        assertThat(test("{\"name\": \"B\"}", cond(name, Predicate.newBuilder().setLt(sv("a")).build())))
                .isTrue();
        assertThat(FieldExpr.read("a", name).sql()).contains("COLLATE \"C\"");
        assertThat(sortedIds(List.of("{\"name\": \"a\"}", "{\"name\": \"B\"}"),
                        List.of(Sort.newBuilder().setField(name).build())))
                .containsExactly(1, 0);
    }

    @Test
    void multipleOpsOnSameFieldApplyInOrder() throws SQLException {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(0));
        FieldOp set5 = FieldOp.newBuilder().setField(coins).setSet(lv(5)).build();
        assertThat(read(apply("{\"coins\": 100}", set5, inc(coins, 2)), coins)).isEqualTo("7");
    }

    @Test
    void rejectsInvalidFieldName() {
        FieldSpec bad = field("coins'--", ScalarType.LONG);
        assertThatThrownBy(() -> FieldExpr.read("a", bad)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UpdateBuilder.dataExpression(List.of(inc(bad, 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PredicateSql.where("a", cond(bad, Predicate.newBuilder().setEq(lv(1)).build())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SortSql.orderBy("a", List.of(Sort.newBuilder().setField(bad).build()), "a.id"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- additional cases ----------

    @Test
    void withdrawRequiresPositiveAmount() {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(0));
        assertThatThrownBy(() -> UpdateBuilder.dataExpression(List.of(withdraw(coins, 0))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UpdateBuilder.implicitConditions(List.of(withdraw(coins, -5))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void consecutiveWithdrawsAreCheckedTogether() throws SQLException {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(0));
        assertThat(apply("{\"coins\": 50}", withdraw(coins, 30), withdraw(coins, 30))).isNull();
        assertThat(read(apply("{\"coins\": 60}", withdraw(coins, 30), withdraw(coins, 30)), coins))
                .isEqualTo("0");
    }

    /** R4: defaults are typed literals, no parameters, and string literals are injection-safe. */
    @Test
    void defaultsAreSafeTypedLiterals() throws SQLException {
        String evil = "it's \\' ? -- /* $$ \"x\"";
        FieldSpec s = field("s", ScalarType.STRING, sv(evil));
        assertThat(FieldExpr.read("a", s).params()).isEmpty();
        assertThat(read("{}", s)).isEqualTo(evil);

        FieldSpec obj = field("o", ScalarType.OBJECT, Value.newBuilder()
                .setMapValue(de.kalypzo.agora.proto.v1.ValueMap.newBuilder().putFields("q", sv(evil)))
                .build());
        assertThat(FieldExpr.read("a", obj).params()).isEmpty();
        assertThat(read("{}", obj)).isEqualTo("{\"q\": \"it's \\\\' ? -- /* $$ \\\"x\\\"\"}");

        FieldSpec min = field("m", ScalarType.LONG, lv(Long.MIN_VALUE));
        assertThat(read("{}", min)).isEqualTo(Long.toString(Long.MIN_VALUE));
        FieldSpec d = field("d", ScalarType.DOUBLE, Value.newBuilder().setDoubleValue(-1.5e300).build());
        assertThat(Double.parseDouble(read("{}", d))).isEqualTo(-1.5e300);
        FieldSpec b = field("b", ScalarType.BOOLEAN, Value.newBuilder().setBoolValue(true).build());
        assertThat(read("{}", b)).isEqualTo("true");
        assertThat(read("{\"b\": false}", b)).isEqualTo("false");
    }

    @Test
    void rejectsDefaultOfWrongType() {
        assertThatThrownBy(() -> FieldExpr.read("a", field("coins", ScalarType.LONG, sv("1"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FieldExpr.read("a", field("s", ScalarType.STRING, sv("a\u0000b"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FieldExpr.read("a'", field("coins", ScalarType.LONG)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** R4: the read expression is usable verbatim as an expression index and the planner matches it (R35 form). */
    @Test
    void readIsUsableAsIndexExpression() throws SQLException {
        assertIndexUsed(field("coins", ScalarType.LONG, lv(0)));
        assertIndexUsed(field("coins", ScalarType.LONG));
    }

    private static void assertIndexUsed(FieldSpec f) throws SQLException {
        String expr = FieldExpr.read("", f).sql();
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("CREATE INDEX sql_semantics_idx ON pantheon_doc ((" + expr + ")) WHERE store = "
                    + SqlLiteral.store("t:idx"));
            StringBuilder plan = new StringBuilder();
            try {
                st.execute("SET enable_seqscan = off");
                SqlFragment where = PredicateSql.where("d", cond(f, Predicate.newBuilder().setGt(lv(10)).build()));
                try (PreparedStatement ps = c.prepareStatement("EXPLAIN SELECT d.id FROM pantheon_doc d WHERE d.store = "
                        + SqlLiteral.store("t:idx") + " AND " + where.sql())) {
                    bind(ps, where.params(), 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) plan.append(rs.getString(1)).append('\n');
                    }
                }
            } finally {
                st.execute("RESET enable_seqscan");
                st.execute("DROP INDEX sql_semantics_idx");
            }
            assertThat(plan.toString()).contains("Index").contains("sql_semantics_idx");
        }
    }

    // ---------- fix round 1: R32-R36 ----------

    /** R32: contains means deep element equality, not jsonb containment. */
    @Test
    void containsUsesDeepElementEquality() throws SQLException {
        FieldSpec sets = field("k", ScalarType.SET, list());
        String nested = "{\"k\": [[1, 2], {\"a\": 1, \"b\": 2}, 3]}";
        assertThat(test(nested, cond(sets, contains(list(lv(1)))))).isFalse();
        assertThat(test(nested, cond(sets, contains(list(lv(1), lv(2)))))).isTrue();
        assertThat(test(nested, cond(sets, contains(map("a", lv(1)))))).isFalse();
        assertThat(test(nested, cond(sets, contains(map("a", lv(1), "b", lv(2)))))).isTrue();
        assertThat(test(nested, cond(sets, contains(lv(3))))).isTrue();
        assertThat(test(nested, cond(sets, contains(Value.newBuilder().setDoubleValue(3.0).build())))).isTrue();
        FieldSpec lists = field("k", ScalarType.LIST);
        assertThat(test(nested, cond(lists, contains(list(lv(1)))))).isFalse();
        assertThat(test("{\"k\": [[[1]]]}", cond(lists, contains(list(list(lv(1))))))).isTrue();
        assertThat(test("{}", cond(lists, contains(lv(1))))).isFalse();
    }

    /** R33: eq/ne on SET is order-independent; LIST stays order-dependent. */
    @Test
    void setEqualityIgnoresOrder() throws SQLException {
        FieldSpec set = field("k", ScalarType.SET);
        FieldSpec lst = field("k", ScalarType.LIST);
        String ab = "{\"k\": [\"a\", \"b\"]}";
        Value ba = list(sv("b"), sv("a"));
        assertThat(test(ab, cond(set, Predicate.newBuilder().setEq(ba).build()))).isTrue();
        assertThat(test(ab, cond(set, Predicate.newBuilder().setNe(ba).build()))).isFalse();
        assertThat(test(ab, cond(set, Predicate.newBuilder().setEq(list(sv("a"))).build()))).isFalse();
        assertThat(test(ab, cond(set, Predicate.newBuilder().setNe(list(sv("a"), sv("c"))).build()))).isTrue();
        assertThat(test(ab, cond(lst, Predicate.newBuilder().setEq(ba).build()))).isFalse();
        assertThat(test(ab, cond(lst, Predicate.newBuilder().setEq(list(sv("a"), sv("b"))).build()))).isTrue();
        assertThat(test("{\"k\": [[1, 2], 3]}", cond(set, Predicate.newBuilder().setEq(list(lv(3), list(lv(1), lv(2)))).build())))
                .isTrue();
        assertThat(test("{\"k\": [[2, 1]]}", cond(set, Predicate.newBuilder().setEq(list(list(lv(1), lv(2)))).build())))
                .isFalse();
        assertThat(test("{}", cond(set, Predicate.newBuilder().setEq(list()).build()))).isFalse();
        assertThat(test("{}", cond(set, Predicate.newBuilder().setNe(list()).build()))).isFalse();
        FieldSpec withDefault = field("k", ScalarType.SET, list());
        assertThat(test("{}", cond(withDefault, Predicate.newBuilder().setEq(list()).build()))).isTrue();
    }

    /** R34: inc/withdraw on a missing field without default start at 0. */
    @Test
    void incStartsAtZeroWithoutDefault() throws SQLException {
        FieldSpec l = field("l", ScalarType.LONG);
        assertThat(apply("{}", inc(l, 2))).isEqualTo("{\"l\": 2}");
        FieldSpec d = field("d", ScalarType.DOUBLE);
        assertThat(apply("{}", inc(d, -3))).isEqualTo("{\"d\": -3}");
    }

    /** R35: where() is always true or false, never SQL NULL. */
    @Test
    void whereNeverYieldsNull() throws SQLException {
        FieldSpec l = field("l", ScalarType.LONG);
        FieldSpec s = field("s", ScalarType.STRING);
        FieldSpec set = field("k", ScalarType.SET);
        FieldSpec obj = field("o", ScalarType.OBJECT);
        List<Condition> conds = List.of(
                cond(l, Predicate.newBuilder().setEq(lv(1)).build()),
                cond(l, Predicate.newBuilder().setNe(lv(1)).build()),
                cond(l, Predicate.newBuilder().setGt(lv(1)).build()),
                cond(l, Predicate.newBuilder().setLte(lv(1)).build()),
                cond(s, Predicate.newBuilder().setLt(sv("a")).build()),
                cond(l, Predicate.newBuilder().setInValues(ValueList.newBuilder()
                        .addValues(lv(1)).addValues(Value.newBuilder().setNullValue(true))).build()),
                cond(set, contains(lv(1))),
                cond(set, Predicate.newBuilder().setEq(list()).build()),
                cond(obj, Predicate.newBuilder().setEq(map("a", lv(1))).build()),
                cond(l, Predicate.newBuilder().setExists(true).build()));
        for (Condition c : conds) {
            assertThat(isNull("{}", c)).as(c.toString()).isFalse();
            assertThat(isNull("{\"l\": 2, \"s\": \"b\", \"k\": [2], \"o\": {}}", c)).as(c.toString()).isFalse();
        }
        assertThat(isNull("{\"l\": 2}", cond(l, Predicate.newBuilder().setInValues(ValueList.newBuilder()
                .addValues(lv(1)).addValues(Value.newBuilder().setNullValue(true))).build()))).isFalse();
    }

    /** R36: overflow in implicitConditions and non-finite DOUBLE defaults are IllegalArgumentException. */
    @Test
    void overflowAndNonFiniteDefaultsAreRejected() {
        FieldSpec coins = field("coins", ScalarType.LONG, lv(0));
        assertThatThrownBy(() -> UpdateBuilder.implicitConditions(List.of(inc(coins, Long.MIN_VALUE), withdraw(coins, 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UpdateBuilder.implicitConditions(List.of(inc(coins, Long.MAX_VALUE), inc(coins, 1))))
                .isInstanceOf(IllegalArgumentException.class);
        for (double bad : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            FieldSpec d = field("d", ScalarType.DOUBLE, Value.newBuilder().setDoubleValue(bad).build());
            assertThatThrownBy(() -> FieldExpr.read("a", d)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void sortIsNullsLastWithIdTieBreak() throws SQLException {
        FieldSpec level = field("level", ScalarType.LONG);
        List<String> rows = List.of("{\"level\": 2}", "{}", "{\"level\": 5}", "{\"level\": 2}");
        assertThat(sortedIds(rows, List.of(Sort.newBuilder().setField(level).build()))).containsExactly(0, 3, 2, 1);
        assertThat(sortedIds(rows, List.of(Sort.newBuilder().setField(level).setDescending(true).build())))
                .containsExactly(2, 0, 3, 1);
    }

    @Test
    void inMatchesListedValues() throws SQLException {
        FieldSpec level = field("level", ScalarType.LONG, lv(1));
        Predicate in = Predicate.newBuilder()
                .setInValues(ValueList.newBuilder().addValues(lv(1)).addValues(lv(3)))
                .build();
        assertThat(test("{}", cond(level, in))).isTrue();
        assertThat(test("{\"level\": 2}", cond(level, in))).isFalse();
        assertThat(test("{}", cond(level, Predicate.newBuilder().setInValues(ValueList.getDefaultInstance()).build())))
                .isFalse();
    }

    @Test
    void existsIgnoresDefault() throws SQLException {
        FieldSpec level = field("level", ScalarType.LONG, lv(1));
        Predicate exists = Predicate.newBuilder().setExists(true).build();
        assertThat(test("{}", cond(level, exists))).isFalse();
        assertThat(test("{\"level\": 1}", cond(level, exists))).isTrue();
    }

    // ---------- harness ----------

    /** Applies the ops (plus implicit conditions) to a fresh row; returns the new data, or null if not applied. */
    private static String apply(String initial, FieldOp... ops) throws SQLException {
        List<FieldOp> list = List.of(ops);
        String id = "r" + ids.incrementAndGet();
        SqlFragment expr = UpdateBuilder.dataExpression(list);
        List<Object> params = new ArrayList<>(expr.params());
        StringBuilder sql = new StringBuilder("UPDATE pantheon_doc t SET data = ")
                .append(expr.sql())
                .append(" WHERE t.store = 't:s' AND t.id = ?");
        params.add(id);
        for (Condition c : UpdateBuilder.implicitConditions(list)) {
            SqlFragment w = PredicateSql.where("t", c);
            sql.append(" AND ").append(w.sql());
            params.addAll(w.params());
        }
        sql.append(" RETURNING data::text");
        try (Connection c = ds.getConnection()) {
            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO pantheon_doc (store, id, data) VALUES ('t:s', ?, ?::jsonb)")) {
                ins.setString(1, id);
                ins.setString(2, initial);
                ins.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                bind(ps, params, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        }
    }

    private static String read(String data, FieldSpec f) throws SQLException {
        SqlFragment r = FieldExpr.read("a", f);
        return single("SELECT (" + r.sql() + ")::text FROM (SELECT ?::jsonb AS data) a", r.params(), data);
    }

    private static boolean test(String data, Condition c) throws SQLException {
        SqlFragment w = PredicateSql.where("a", c);
        String sql = "SELECT EXISTS (SELECT 1 FROM (SELECT ?::jsonb AS data) a WHERE " + w.sql() + ")";
        return "t".equals(single(sql, w.params(), data));
    }

    private static boolean isNull(String data, Condition c) throws SQLException {
        SqlFragment w = PredicateSql.where("a", c);
        // data is the first placeholder, the predicate's parameters follow
        return "t".equals(single("SELECT w.x IS NULL FROM (SELECT ?::jsonb AS data) a"
                + " CROSS JOIN LATERAL (SELECT (" + w.sql() + ") AS x) w", w.params(), data));
    }

    private static String single(String sql, List<Object> params, String data) throws SQLException {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, data);
            bind(ps, params, 2);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    /** Sorts the given rows (ids = list index) and returns the ids in result order. */
    private static List<Integer> sortedIds(List<String> rows, List<Sort> sorts) throws SQLException {
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) values.append(", ");
            values.append("(").append(i).append(", ?::jsonb)");
        }
        SqlFragment order = SortSql.orderBy("a", sorts, "a.id");
        String sql = "SELECT a.id FROM (VALUES " + values + ") AS a(id, data) " + order.sql();
        List<Integer> out = new ArrayList<>();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < rows.size(); i++) ps.setString(i + 1, rows.get(i));
            bind(ps, order.params(), rows.size() + 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getInt(1));
            }
        }
        return out;
    }

    private static void bind(PreparedStatement ps, List<Object> params, int from) throws SQLException {
        for (int i = 0; i < params.size(); i++) ps.setObject(from + i, params.get(i));
    }

    private static FieldSpec field(String name, ScalarType type) {
        return FieldSpec.newBuilder().setNamespace("t").setName(name).setType(type).build();
    }

    private static FieldSpec field(String name, ScalarType type, Value def) {
        return field(name, type).toBuilder().setDefaultValue(def).build();
    }

    private static Condition cond(FieldSpec f, Predicate p) {
        return Condition.newBuilder().setField(f).setPredicate(p).build();
    }

    private static Value lv(long v) {
        return Value.newBuilder().setLongValue(v).build();
    }

    private static Value sv(String v) {
        return Value.newBuilder().setStringValue(v).build();
    }

    private static Value list(Value... vs) {
        return Value.newBuilder().setListValue(ValueList.newBuilder().addAllValues(List.of(vs))).build();
    }

    private static Predicate contains(Value v) {
        return Predicate.newBuilder().setContains(v).build();
    }

    private static Value map(Object... kv) {
        var m = de.kalypzo.agora.proto.v1.ValueMap.newBuilder();
        for (int i = 0; i < kv.length; i += 2) m.putFields((String) kv[i], (Value) kv[i + 1]);
        return Value.newBuilder().setMapValue(m).build();
    }

    private static FieldOp inc(FieldSpec f, long n) {
        return FieldOp.newBuilder().setField(f).setInc(n).build();
    }

    private static FieldOp withdraw(FieldSpec f, long n) {
        return FieldOp.newBuilder().setField(f).setWithdraw(n).build();
    }

    private static FieldOp max(FieldSpec f, Value v) {
        return FieldOp.newBuilder().setField(f).setMax(v).build();
    }

    private static FieldOp min(FieldSpec f, Value v) {
        return FieldOp.newBuilder().setField(f).setMin(v).build();
    }

    private static FieldOp add(FieldSpec f, Value v) {
        return FieldOp.newBuilder().setField(f).setAdd(v).build();
    }

    private static FieldOp remove(FieldSpec f, Value v) {
        return FieldOp.newBuilder().setField(f).setRemove(v).build();
    }
}

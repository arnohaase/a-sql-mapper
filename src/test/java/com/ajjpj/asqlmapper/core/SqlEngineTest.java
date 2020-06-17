package com.ajjpj.asqlmapper.core;

import static com.ajjpj.asqlmapper.core.SqlSnippet.sql;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
import com.ajjpj.asqlmapper.core.common.CommonPrimitiveHandlers;
import com.ajjpj.asqlmapper.core.common.RawRowExtractor;
import com.ajjpj.asqlmapper.core.common.ScalarRowExtractor;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class SqlEngineTest extends AbstractDatabaseTest {

    @BeforeEach
    void setUp() throws SQLException {
        executeUpdate("CREATE TABLE person(id BIGSERIAL PRIMARY KEY, name VARCHAR(100))");
    }

    @AfterEach
    void tearDown() throws SQLException {
        executeUpdate("DROP TABLE person");
    }

    @Test void testExecuteUpdate() {
        final SqlEngine engine = SqlEngine.create();

        assertEquals(1, engine.executeUpdate(conn, "INSERT INTO person (id, name) VALUES (?,?)", 2, "Arno"));

        assertEquals(0, engine.executeUpdate(conn, "UPDATE person SET name=? WHERE id=?", "Arno", 1));
        assertEquals(1, engine.executeUpdate(conn, "UPDATE person SET name=? WHERE id=?", "Arno", 2));

        assertEquals(0, engine.executeUpdate(conn, sql("UPDATE person SET name=? WHERE id=?", "Arno", 1)));
        assertEquals(1, engine.executeUpdate(conn, sql("UPDATE person SET name=? WHERE id=?", "Arno", 2)));

        assertThrows(IllegalStateException.class, () -> engine.executeUpdate("UPDATE person SET name=? WHERE id=?", "Arno", 1));
        assertThrows(IllegalStateException.class, () -> engine.executeUpdate(sql("UPDATE person SET name=? WHERE id=?", "Arno", 1)));

        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);

        assertEquals(0, e2.executeUpdate("UPDATE person SET name=? WHERE id=?", "Arno", 1));
        assertEquals(1, e2.executeUpdate("UPDATE person SET name=? WHERE id=?", "Arno", 2));

        assertEquals(0, e2.executeUpdate(sql("UPDATE person SET name=? WHERE id=?", "Arno", 1)));
        assertEquals(1, e2.executeUpdate(sql("UPDATE person SET name=? WHERE id=?", "Arno", 2)));
    }

    @Test void testExecuteLargeUpdate() {
        final SqlEngine engine = SqlEngine.create();

        assertEquals(1, engine.executeLargeUpdate(conn, "INSERT INTO person (id, name) VALUES (?,?)", 2, "Arno"));

        assertEquals(0, engine.executeLargeUpdate(conn, "UPDATE person SET name=? WHERE id=?", "Arno", 1));
        assertEquals(1, engine.executeLargeUpdate(conn, "UPDATE person SET name=? WHERE id=?", "Arno", 2));

        assertEquals(0, engine.executeLargeUpdate(conn, sql("UPDATE person SET name=? WHERE id=?", "Arno", 1)));
        assertEquals(1, engine.executeLargeUpdate(conn, sql("UPDATE person SET name=? WHERE id=?", "Arno", 2)));

        assertThrows(IllegalStateException.class, () -> engine.executeLargeUpdate("UPDATE person SET name=? WHERE id=?", "Arno", 1));
        assertThrows(IllegalStateException.class, () -> engine.executeLargeUpdate(sql("UPDATE person SET name=? WHERE id=?", "Arno", 1)));

        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);

        assertEquals(0, e2.executeLargeUpdate("UPDATE person SET name=? WHERE id=?", "Arno", 1));
        assertEquals(1, e2.executeLargeUpdate("UPDATE person SET name=? WHERE id=?", "Arno", 2));

        assertEquals(0, e2.executeLargeUpdate(sql("UPDATE person SET name=? WHERE id=?", "Arno", 1)));
        assertEquals(1, e2.executeLargeUpdate(sql("UPDATE person SET name=? WHERE id=?", "Arno", 2)));
    }

    private void createPerson(int id, String name) {
        SqlEngine.create().executeUpdate(conn, "INSERT INTO person (id, name) VALUES (?,?)", id, name);
    }

    @Test void testBatchParams() {
        createPerson(1, "Arno1");
        createPerson(2, "Arno2");

        final SqlEngine engine = SqlEngine.create();

        {
            final int[] results = engine.executeBatch(conn, "UPDATE PERSON SET name=? WHERE id=?", AList.of(AList.of("A", 1), AList.of("B", 2), AList.of("C", 3)));
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }

        assertThrows(IllegalStateException.class, () -> engine.executeBatch("UPDATE PERSON SET name=? WHERE id=?", AList.of(AList.of("A", 1), AList.of("B", 2), AList.of("C", 3))));
        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);
        {
            final int[] results = e2.executeBatch("UPDATE PERSON SET name=? WHERE id=?", AList.of(AList.of("A", 1), AList.of("B", 2), AList.of("C", 3)));
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }
    }

    @Test void testBatchSnippets() {
        createPerson(1, "Arno1");
        createPerson(2, "Arno2");

        final SqlEngine engine = SqlEngine.create();

        final List<SqlSnippet> updates =  AList.of(
                sql("UPDATE PERSON SET name=? WHERE id=?", "A", 1),
                sql("UPDATE PERSON SET name=? WHERE id=?", "B", 2),
                sql("UPDATE PERSON SET name=? WHERE id=?", "C", 3)
        );

        {
            final int[] results = engine.executeBatch(conn, updates);
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }

        assertThrows(IllegalStateException.class, () -> engine.executeBatch(updates));
        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);
        {
            final int[] results = e2.executeBatch(updates);
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }
    }

    @Test void testLargeBatchParams() {
        createPerson(1, "Arno1");
        createPerson(2, "Arno2");

        final SqlEngine engine = SqlEngine.create();

        {
            final long[] results = engine.executeLargeBatch(conn, "UPDATE PERSON SET name=? WHERE id=?", AList.of(AList.of("A", 1), AList.of("B", 2), AList.of("C", 3)));
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }

        assertThrows(IllegalStateException.class, () -> engine.executeLargeBatch("UPDATE PERSON SET name=? WHERE id=?", AList.of(AList.of("A", 1), AList.of("B", 2), AList.of("C", 3))));
        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);
        {
            final long[] results = e2.executeLargeBatch("UPDATE PERSON SET name=? WHERE id=?", AList.of(AList.of("A", 1), AList.of("B", 2), AList.of("C", 3)));
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }
    }

    @Test void testLargeBatchSnippets() {
        createPerson(1, "Arno1");
        createPerson(2, "Arno2");

        final SqlEngine engine = SqlEngine.create();

        final List<SqlSnippet> updates =  AList.of(
                sql("UPDATE PERSON SET name=? WHERE id=?", "A", 1),
                sql("UPDATE PERSON SET name=? WHERE id=?", "B", 2),
                sql("UPDATE PERSON SET name=? WHERE id=?", "C", 3)
        );

        {
            final long[] results = engine.executeLargeBatch(conn, updates);
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }

        assertThrows(IllegalStateException.class, () -> engine.executeLargeBatch(updates));
        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);
        {
            final long[] results = e2.executeLargeBatch(updates);
            assertEquals(3, results.length);
            assertEquals(1, results[0]);
            assertEquals(1, results[1]);
            assertEquals(0, results[2]);
        }
    }

    @Test void testInsert() {
        final SqlEngine engine = SqlEngine.create();
        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);

        final long pk1 = engine.insert(conn, Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person (name) VALUES (?)", "arno"), "id");
        final long pk2 = engine.insert(conn, Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person (name) VALUES (?)", "arno"), Arrays.asList("id"));

        assertThrows(IllegalStateException.class, () -> engine.insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO pseron (name) VALUES (?)", "arno"), "id"));
        assertThrows(IllegalStateException.class, () -> engine.insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO pseron (name) VALUES (?)", "arno"), Arrays.asList("id")));

        long pk3 = e2.insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person(name) VALUES (?)", "arno"), "id");
        long pk4 = e2.insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person(name) VALUES (?)", "arno"), Arrays.asList("id"));

        assertEquals(AList.of(pk1, pk2, pk3, pk4), e2.longQuery("SELECT id FROM person ORDER BY id").list());
    }

    @Test void testInsertMulti() {
        final SqlEngine engine = SqlEngine.create();
        final SqlEngine e2 = engine.withDefaultConnectionSupplier(() -> conn);

        final List<Long> pks = new ArrayList<>();

        pks.addAll(engine.insertMulti(conn, Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person (name) VALUES (?),(?)", "arno", "arno2"), "id"));
        pks.addAll(engine.insertMulti(conn, Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person (name) VALUES (?),(?)", "arno", "arno2"), Arrays.asList("id")));

        assertThrows(IllegalStateException.class, () -> engine.insertMulti(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO pseron (name) VALUES (?)", "arno"), "id"));
        assertThrows(IllegalStateException.class, () -> engine.insertMulti(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO pseron (name) VALUES (?)", "arno"), Arrays.asList("id")));

        pks.addAll(e2.insertMulti(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person (name) VALUES (?),(?)", "arno", "arno2"), "id"));
        pks.addAll(e2.insertMulti(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql("INSERT INTO person (name) VALUES (?),(?)", "arno", "arno2"), Arrays.asList("id")));

        assertEquals(pks, e2.longQuery("SELECT id FROM person ORDER BY id").list());
    }

    @Test void testInsertCompositePk() throws SQLException {
        executeUpdate("CREATE TABLE tbl_composite_pk(a bigserial, b bigserial, primary key(a,b))");

        try {
            final SqlEngine engine = SqlEngine.create().withDefaultConnectionSupplier(() -> conn);
            final List<SqlRow> pks = new ArrayList<>();

            pks.add(engine.insert(conn, SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES()"), "a", "b"));
            pks.add(engine.insert(conn, SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES()"), Arrays.asList("a", "b")));

            pks.add(engine.insert(SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES()"), "a", "b"));
            pks.add(engine.insert(SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES()"), Arrays.asList("a", "b")));

            pks.addAll(engine.insertMulti(conn, SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES(),()"), "a", "b"));
            pks.addAll(engine.insertMulti(conn, SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES(),()"), Arrays.asList("a", "b")));

            pks.addAll(engine.insertMulti(SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES(),()"), "a", "b"));
            pks.addAll(engine.insertMulti(SqlRow.class, RawRowExtractor.INSTANCE, sql("INSERT INTO tbl_composite_pk() VALUES(),()"), Arrays.asList("a", "b")));

            assertEquals(pks, engine.rawQuery("SELECT a, b FROM tbl_composite_pk ORDER BY a").list());
        }
        finally {
            executeUpdate("DROP TABLE tbl_composite_pk");
        }
    }

    @Test void testTodo() {
        fail("SqlEngine javadoc");
        fail("javadoc AQuery");
        fail("SqlMapper#120");
        fail("DemoTest#55");
        fail("ToOneDemoTest#44");
    }

    @Test void testInsertIntegerPk() {
        final SqlEngine e = SqlEngine.create();
        final SqlEngine eConn = e.withDefaultConnectionSupplier(() -> conn);
        final SqlEngine eId = e.withDefaultPkName("id");
        final SqlEngine eConnId = eConn.withDefaultPkName("id");

        final List<Integer> pks = new ArrayList<>();

        pks.add(e.insertIntegerPkInCol(conn, "id", "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(e.insertIntegerPkInCol(conn, "id", sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> e.insertIntegerPkInCol("id", "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> e.insertIntegerPkInCol("id", sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eConn.insertIntegerPkInCol("id", "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eConn.insertIntegerPkInCol("id", sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> e.insertIntegerPk(conn, "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> e.insertIntegerPk(conn, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eId.insertIntegerPk(conn, "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eId.insertIntegerPk(conn, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> eId.insertIntegerPk("INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> eId.insertIntegerPk(sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        assertThrows(IllegalStateException.class, () -> eConn.insertIntegerPk("INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> eConn.insertIntegerPk(sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eConnId.insertIntegerPk("INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eConnId.insertIntegerPk(sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertEquals(pks, eConn.intQuery("SELECT id FROM person ORDER BY id").list());
    }

    @Test void testInsertLongPk() {
        final SqlEngine e = SqlEngine.create();
        final SqlEngine eConn = e.withDefaultConnectionSupplier(() -> conn);
        final SqlEngine eId = e.withDefaultPkName("id");
        final SqlEngine eConnId = eConn.withDefaultPkName("id");

        final List<Long> pks = new ArrayList<>();

        pks.add(e.insertLongPkInCol(conn, "id", "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(e.insertLongPkInCol(conn, "id", sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> e.insertLongPkInCol("id", "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> e.insertLongPkInCol("id", sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eConn.insertLongPkInCol("id", "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eConn.insertLongPkInCol("id", sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> e.insertLongPk(conn, "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> e.insertLongPk(conn, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eId.insertLongPk(conn, "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eId.insertLongPk(conn, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> eId.insertLongPk("INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> eId.insertLongPk(sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        assertThrows(IllegalStateException.class, () -> eConn.insertLongPk("INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> eConn.insertLongPk(sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eConnId.insertLongPk("INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eConnId.insertLongPk(sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertEquals(pks, eConn.longQuery("SELECT id FROM person ORDER BY id").list());
    }

    @Test void testInsertSingleColPk() {
        final SqlEngine e = SqlEngine.create();
        final SqlEngine eConn = e.withDefaultConnectionSupplier(() -> conn);
        final SqlEngine eId = e.withDefaultPkName("id");
        final SqlEngine eConnId = eConn.withDefaultPkName("id");

        final List<Long> pks = new ArrayList<>();

        pks.add(e.insertSingleColPkInCol(conn, "id", Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(e.insertSingleColPkInCol(conn, "id", Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> e.insertSingleColPkInCol("id", Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> e.insertSingleColPkInCol("id", Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eConn.insertSingleColPkInCol("id", Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eConn.insertSingleColPkInCol("id", Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> e.insertSingleColPk(conn, Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> e.insertSingleColPk(conn, Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eId.insertSingleColPk(conn, Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eId.insertSingleColPk(conn, Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertThrows(IllegalStateException.class, () -> eId.insertSingleColPk(Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> eId.insertSingleColPk(Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        assertThrows(IllegalStateException.class, () -> eConn.insertSingleColPk(Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        assertThrows(IllegalStateException.class, () -> eConn.insertSingleColPk(Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));
        pks.add(eConnId.insertSingleColPk(Long.class, "INSERT INTO person (name) VALUES (?)", "arno"));
        pks.add(eConnId.insertSingleColPk(Long.class, sql("INSERT INTO person (name)"), sql("VALUES (?)", "arno")));

        assertEquals(pks, eConn.longQuery("SELECT id FROM person ORDER BY id").list());
    }

    @Test void testScalarQuery() {
        final SqlEngine engine = SqlEngine.create();

        assertEquals(0, engine.scalarQuery(Integer.class, "SELECT COUNT(*) FROM person WHERE name LIKE ?", "%").single(conn).intValue());
        assertEquals(0, engine.scalarQuery(Integer.class, sql("SELECT COUNT(*) FROM person WHERE name"), sql("LIKE ?", "%")).single(conn).intValue());
    }

    @Test void testAQuerySingle() {
        createPerson(1, "Arno");
        createPerson(2, "B");
        createPerson(3, "B");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        assertEquals("Arno", e.stringQuery("SELECT name FROM person WHERE id=?", 1).single(conn));
        assertThrows(IllegalStateException.class, () -> e.stringQuery("SELECT name FROM person WHERE id=?", 1).single());
        assertEquals("Arno", e2.stringQuery("SELECT name FROM person WHERE id=?", 1).single());

        assertThrows(IllegalStateException.class, () -> e.stringQuery("SELECT name FROM person").single(conn));
        assertThrows(NoSuchElementException.class, () -> e.stringQuery("SELECT name FROM person WHERE id=?", 99).single(conn));
    }

    @Test void testAQueryOptional() {
        createPerson(1, "Arno");
        createPerson(2, "B");
        createPerson(3, "B");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        assertEquals(AOption.of("Arno"), e.stringQuery("SELECT name FROM person WHERE id=?", 1).optional(conn));
        assertThrows(IllegalStateException.class, () -> e.stringQuery("SELECT name FROM person WHERE id=?", 1).optional());
        assertEquals(AOption.of("Arno"), e2.stringQuery("SELECT name FROM person WHERE id=?", 1).optional());

        assertThrows(IllegalStateException.class, () -> e.stringQuery("SELECT name FROM person").optional(conn));
        assertEquals(AOption.empty(), e.stringQuery("SELECT name FROM person WHERE id=?", 99).optional(conn));
    }

    @Test void testAQueryFirst() {
        createPerson(1, "Arno");
        createPerson(2, "B");
        createPerson(3, "B");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        assertEquals(AOption.of("Arno"), e.stringQuery("SELECT name FROM person WHERE id=?", 1).first(conn));
        assertThrows(IllegalStateException.class, () -> e.stringQuery("SELECT name FROM person WHERE id=?", 1).first());
        assertEquals(AOption.of("Arno"), e2.stringQuery("SELECT name FROM person WHERE id=?", 1).first());

        assertEquals(AOption.of("B"), e.stringQuery("SELECT name FROM person WHERE id>1").first(conn));
        assertEquals(AOption.empty(), e.stringQuery("SELECT name FROM person WHERE id=?", 99).first(conn));
    }

    @Test void testAQueryList() {
        createPerson(1, "Arno");
        createPerson(2, "Arno");
        createPerson(3, "Bert");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        assertEquals(AList.of(1, 2), e.intQuery("SELECT id FROM person WHERE name=?", "Arno").list(conn));
        assertThrows(IllegalStateException.class, () -> e.intQuery("SELECT id FROM person WHERE name=?", "Arno").list());
        assertEquals(AList.of(1, 2), e2.intQuery("SELECT id FROM person WHERE name=?", "Arno").list());
    }

    @Test void testAQueryForEach() {
        createPerson(1, "Arno");
        createPerson(2, "Arno");
        createPerson(3, "Bert");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        final List<Integer> result = new ArrayList<>();

        e.intQuery("SELECT id FROM person WHERE name=?", "Arno").forEach(conn, result::add);
        assertThrows(IllegalStateException.class, () -> e.intQuery("SELECT id FROM person WHERE name=?", "Arno").forEach(result::add));
        e2.intQuery("SELECT id FROM person WHERE name=?", "Arno").forEach(result::add);

        assertEquals(AList.of(1, 2, 1, 2), result);
    }

    @Test void testAQueryForEachWithRowAccess() {
        createPerson(1, "Arno");
        createPerson(2, "Arno");
        createPerson(3, "Bert");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        final List<Integer> result = new ArrayList<>();

        e.intQuery("SELECT id FROM person WHERE name=?", "Arno").forEachWithRowAccess(conn, (i, row) -> {
            assertEquals(i, row.getInt(0));
            assertEquals(i, row.getInt("id"));
            result.add(i);
        });
        assertThrows(IllegalStateException.class, () -> e.intQuery("SELECT id FROM person WHERE name=?", "Arno").forEachWithRowAccess((i, row) -> {
            assertEquals(i, row.getInt(0));
            assertEquals(i, row.getInt("id"));
            result.add(i);
        }));
        e2.intQuery("SELECT id FROM person WHERE name=?", "Arno").forEachWithRowAccess((i, row) -> {
            assertEquals(i, row.getInt(0));
            assertEquals(i, row.getInt("id"));
            result.add(i);
        });

        assertEquals(AList.of(1, 2, 1, 2), result);
    }

    @Test void testAQueryCollect() {
        createPerson(1, "Arno");
        createPerson(2, "Arno");
        createPerson(3, "Bert");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        assertEquals(AList.of(1, 2), e.intQuery("SELECT id FROM person WHERE name=?", "Arno").collect(conn, Collectors.toList()));
        assertThrows(IllegalStateException.class, () -> e.intQuery("SELECT id FROM person WHERE name=?", "Arno").collect(Collectors.toList()));
        assertEquals(AList.of(1, 2), e2.intQuery("SELECT id FROM person WHERE name=?", "Arno").collect(Collectors.toList()));
    }

    @Test void testAQueryStream() {
        createPerson(1, "Arno");
        createPerson(2, "Arno");
        createPerson(3, "Bert");

        final SqlEngine e = SqlEngine.create();
        final SqlEngine e2 = e.withDefaultConnectionSupplier(() -> conn);

        assertEquals(AList.of(1, 2), e.intQuery("SELECT id FROM person WHERE name=?", "Arno").stream(conn).collect(Collectors.toList()));
        //noinspection ResultOfMethodCallIgnored
        assertThrows(IllegalStateException.class, () -> e.intQuery("SELECT id FROM person WHERE name=?", "Arno").stream().collect(Collectors.toList()));
        assertEquals(AList.of(1, 2), e2.intQuery("SELECT id FROM person WHERE name=?", "Arno").stream().collect(Collectors.toList()));
    }

    @Test void testInjectedProperty() {
        fail("todo");
    }

    @Test void testRawQuery() {
        createPerson(1, "Arno");
        createPerson(2, "Bert");
        createPerson(3, "Curt");

        final SqlEngine engine = SqlEngine.create();

        assertEquals(AList.of("Arno", "Bert", "Curt"), engine.rawQuery("SELECT name FROM person ORDER BY id").list(conn).map(row -> row.getString(0)));
        assertEquals(AList.of("Arno", "Bert", "Curt"), engine.rawQuery(sql("SELECT name FROM person"), sql("ORDER BY id")).list(conn).map(row -> row.getString(0)));
    }

    @Test void testLongQuery() {
        final SqlEngine engine = SqlEngine.create();

        assertEquals(0, engine.longQuery("SELECT COUNT(*) FROM person WHERE name LIKE ?", "%").single(conn).longValue());
        assertEquals(0, engine.longQuery(sql("SELECT COUNT(*) FROM person WHERE name"), sql("LIKE ?", "%")).single(conn).longValue());
    }

    @Test void testIntQuery() {
        final SqlEngine engine = SqlEngine.create();

        assertEquals(0, engine.intQuery("SELECT COUNT(*) FROM person WHERE name LIKE ?", "%").single(conn).intValue());
        assertEquals(0, engine.intQuery(sql("SELECT COUNT(*) FROM person WHERE name"), sql("LIKE ?", "%")).single(conn).intValue());
    }

    @Test void testStringQuery() {
        createPerson(1, "Arno");

        final SqlEngine engine = SqlEngine.create();

        assertEquals("Arno", engine.stringQuery("SELECT name FROM person WHERE name LIKE ?", "%").single(conn));
        assertEquals("Arno", engine.stringQuery(sql("SELECT name FROM person WHERE name"), sql("LIKE ?", "%")).single(conn));
    }

    @Test void testUuidQuery() {
        final UUID uuid = UUID.randomUUID();
        createPerson(1, uuid.toString());

        final SqlEngine engine = SqlEngine
                .create()
                .withPrimitiveHandler(CommonPrimitiveHandlers.UUID_AS_STRING_HANDLER);

        assertEquals(uuid, engine.uuidQuery("SELECT name FROM person").single(conn));
        assertEquals(uuid, engine.uuidQuery(sql("SELECT name"), sql("FROM person")).single(conn));
    }

    @Test void testBooleanQuery() {
        createPerson(1, "Arno");

        final SqlEngine engine = SqlEngine.create();

        assertTrue(engine.booleanQuery("SELECT 1<2 FROM person").single(conn));
        assertTrue(engine.booleanQuery(sql("SELECT 1<2"), sql("FROM person")).single(conn));
    }

    @Test void testDoubleQuery() {
        createPerson(1, "Arno");

        final SqlEngine engine = SqlEngine.create();

        assertEquals(1.0, engine.doubleQuery("SELECT id FROM person").single(conn), .000001);
        assertEquals(1.0, engine.doubleQuery(sql("SELECT id"), sql("FROM person")).single(conn), .000001);
    }

    @Test void testRawTypeMapping() {
        fail("todo");
    }

    @Test void testConfig() {
        fail("todo");
    }

    @Test void testRowExtractorFor() {
        fail("todo");
    }
}

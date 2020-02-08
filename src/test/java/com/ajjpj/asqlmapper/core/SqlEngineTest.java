package com.ajjpj.asqlmapper.core;

import static com.ajjpj.asqlmapper.core.SqlSnippet.sql;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.AbstractDatabaseTest;
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
        fail("make AInsert package visible");
        fail("remove AInsert - it is obsolete");
        fail("insert methods with explicit Connection");
        fail("SqlEngine javadoc");
        fail("SqlMapper#120");
        fail("DemoTest#55");
        fail("ToOneDemoTest#44");
    }

    @Test void testInsertUuidPk() {
        fail("todo");
    }

    @Test void testInsertStringPk() {
        fail("todo");
    }

    @Test void testInsertIntegerPk() {
        fail("todo");
    }

    @Test void testInsertLongPk() {
        fail("todo");
    }

    @Test void testInsertSingleColPk() {
        fail("todo");
    }

    @Test void testScalarQuery() {
        fail("todo");
    }

    @Test void testQuery() {
        fail("todo");
    }

    @Test void testRawQuery() {
        fail("todo");
    }

    @Test void testLongQuery() {
        fail("todo");
    }

    @Test void testIntQuery() {
        fail("todo");
    }

    @Test void testStringQuery() {
        fail("todo");
    }

    @Test void testUuidQuery() {
        fail("todo");
    }

    @Test void testBooleanQuery() {
        fail("todo");
    }

    @Test void testDoubleQuery() {
        fail("todo");
    }

    @Test void testConfig() {
        fail("todo");
    }

    @Test void testRowExtractorFor() {
        fail("todo");
    }
}

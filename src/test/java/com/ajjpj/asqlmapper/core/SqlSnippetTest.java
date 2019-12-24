package com.ajjpj.asqlmapper.core;

import static com.ajjpj.asqlmapper.testutil.CollectionUtils.listOf;
import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.*;

import com.ajjpj.acollections.AIterator;
import org.junit.jupiter.api.Test;
import static com.ajjpj.asqlmapper.core.SqlSnippet.*;

import java.util.ArrayList;
import java.util.Collections;

public class SqlSnippetTest {
    @Test void testEmpty() {
        assertEquals("", SqlSnippet.EMPTY.getSql());
        assertTrue(SqlSnippet.EMPTY.getParams().isEmpty());
    }

    @Test void testCreate() {
        {
            final SqlSnippet s = sql("SELECT * FROM my_table");
            assertEquals("SELECT * FROM my_table", s.getSql());
            assertTrue(s.getParams().isEmpty());
        }
        {
            final SqlSnippet s = sql("UPDATE tbl SET name=? WHERE id=?", "Arno", 123L);
            assertEquals("UPDATE tbl SET name=? WHERE id=?", s.getSql());
            assertEquals(listOf("Arno", 123L), s.getParams());
        }
    }

    @Test void testEqualsHashCode() {
        assertEquals(sql("a"), sql("a"));
        assertEquals(sql("a").hashCode(), sql("a").hashCode());

        assertNotEquals(sql("a"), sql("b"));
        assertNotEquals(sql("a").hashCode(), sql("b").hashCode());

        assertEquals(sql("a", 1), sql("a", 1));
        assertEquals(sql("a", 1).hashCode(), sql("a", 1).hashCode());

        assertNotEquals(sql("a", 1), sql("a"));
        assertNotEquals(sql("a", 1).hashCode(), sql("a").hashCode());

        assertNotEquals(sql("a", 1), sql("a", 2));
        assertNotEquals(sql("a", 1).hashCode(), sql("a", 2).hashCode());

        assertNotEquals(sql("a", 1), sql("a", 1, 2));
        assertNotEquals(sql("a", 1).hashCode(), sql("a", 1, 2).hashCode());
    }

    @Test void testConcat() {
        assertEquals(sql("a b", 1, 2, 3), concat(sql("a", 1), sql("b", 2, 3)));
        assertEquals(sql("a", 99), concat(sql("a", 99)));
        assertEquals(sql("a  b  c", 9, 8, 7), concat(sql("a "), sql("b", 9, 8), sql(" c", 7)));
    }

    @Test void testConcatColl() {
        assertEquals(sql("a b", 1, 2, 3), concat(listOf(sql("a", 1), sql("b", 2, 3))));
        assertEquals(sql("a", 99), concat(listOf(sql("a", 99))));
        assertEquals(sql("a  b  c", 9, 8, 7), concat(listOf(sql("a "), sql("b", 9, 8), sql(" c", 7))));
        
        assertEquals(EMPTY, concat(listOf()));
    }

    @Test void testConcatIterator() {
        assertEquals(sql("a b", 1, 2, 3), concat(listOf(sql("a", 1), sql("b", 2, 3)).iterator()));
        assertEquals(sql("a", 99), concat(listOf(sql("a", 99)).iterator()));
        assertEquals(sql("a  b  c", 9, 8, 7), concat(listOf(sql("a "), sql("b", 9, 8), sql(" c", 7)).iterator()));

        assertEquals(EMPTY, concat(emptyIterator()));
    }

    @Test void testBuilder() {
        fail("todo");
    }

    @Test void testParam() {
        assertEquals(sql("?", 9), param(9));
        assertEquals(sql("?", "asdf"), param("asdf"));
        assertEquals(sql("?", (Object)null), param(null));
    }

    @Test void testParams() {
        assertEquals(sql("?", 2), params(listOf(2)));
        assertEquals(sql("?,?,?", 2, 5, 3), params(listOf(2, 5, 3)));
        assertEquals(EMPTY, params(emptyList()));
    }

    @Test void testParamsIterator() {
        assertEquals(sql("?", 2), params(listOf(2).iterator()));
        assertEquals(sql("?,?,?", 2, 5, 3), params(listOf(2, 5, 3).iterator()));
        assertEquals(EMPTY, params(emptyIterator()));
    }

    @Test void testCommaSeparated() {
        assertEquals(sql("a", 1), commaSeparated(listOf(sql("a", 1))));
        assertEquals(sql("a,b", 1, 2), commaSeparated(listOf(sql("a", 1), sql("b", 2))));
        assertEquals(sql("a,b,c", 1, 2, 3), commaSeparated(listOf(sql("a", 1), sql("b", 2), sql("c", 3))));

        assertEquals(sql("a", 1), commaSeparated(listOf(sql("a", 1)).iterator()));
        assertEquals(sql("a,b", 1, 2), commaSeparated(listOf(sql("a", 1), sql("b", 2)).iterator()));
        assertEquals(sql("a,b,c", 1, 2, 3), commaSeparated(listOf(sql("a", 1), sql("b", 2), sql("c", 3)).iterator()));
    }

    @Test void testCombine() {
        fail("todo");
    }

    @Test void testCombineNoBlank() {
        fail("todo");
    }

    @Test void testInSnippets() {
        assertEquals(sql("IN (a,b,c)", 1, 2, 3), inSnippets(sql("a", 1), sql("b"), sql("c", 2, 3)));
        assertEquals(sql("IN (a)", 1), inSnippets(sql("a", 1)));
    }
    @Test void testInSnippetsCollection() {
        assertEquals(sql("IN (a,b,c)", 1, 2, 3), inSnippets(listOf(sql("a", 1), sql("b"), sql("c", 2, 3))));
        assertEquals(sql("IN (a)", 1), inSnippets(listOf(sql("a", 1))));
        assertThrows(IllegalArgumentException.class, () -> inSnippets(emptyList()));
    }
    @Test void testInSnippetsIterator() {
        assertEquals(sql("IN (a,b,c)", 1, 2, 3), inSnippets(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()));
        assertEquals(sql("IN (a)", 1), inSnippets(listOf(sql("a", 1)).iterator()));
        assertThrows(IllegalArgumentException.class, () -> inSnippets(emptyIterator()));
    }
    
    @Test void testIn() {
        assertEquals(sql("IN (?,?,?)", 1, 2, 3), in(1, 2, 3));
        assertEquals(sql("IN (?)", 1), in(1));
    }
    @Test void testInCollection() {
        assertEquals(sql("IN (?,?,?)", 1, 2, 3), in(listOf(1, 2, 3)));
        assertEquals(sql("IN (?)", 1), in(listOf(1)));
        assertThrows(IllegalArgumentException.class, () -> in(emptyList()));
    }
    @Test void testInIterator() {
        assertEquals(sql("IN (?,?,?)", 1, 2, 3), in(listOf(1, 2, 3).iterator()));
        assertEquals(sql("IN (?)", 1), in(listOf(1).iterator()));
        assertThrows(IllegalArgumentException.class, () -> in(emptyIterator()));
    }

    @Test void testChunkedIn() {
        fail("todo");
    }

    @Test void testAnd() {
        assertEquals(sql("( a AND b )", 1, 2), and(sql("a", 1), sql("b", 2)));
        assertEquals(sql("( a AND b AND c )", 1, 2, 3), and(sql("a", 1), sql("b"), sql("c", 2, 3)));
        assertEquals(sql("a", 1), and(sql("a", 1)));
    }
    @Test void testAndCollection() {
        assertEquals(sql("( a AND b )", 1, 2), and(listOf(sql("a", 1), sql("b", 2))));
        assertEquals(sql("( a AND b AND c )", 1, 2, 3), and(listOf(sql("a", 1), sql("b"), sql("c", 2, 3))));
        assertEquals(sql("a", 1), and(listOf(sql("a", 1))));
        assertEquals(sql("1<>2"), and(emptyList()));
    }
    @Test void testAndIterator() {
        assertEquals(sql("( a AND b )", 1, 2), and(listOf(sql("a", 1), sql("b", 2)).iterator()));
        assertEquals(sql("( a AND b AND c )", 1, 2, 3), and(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()));
        assertEquals(sql("a", 1), and(listOf(sql("a", 1)).iterator()));
        assertEquals(sql("1<>2"), and(emptyIterator()));
    }

    @Test void testOr() {
        assertEquals(sql("( a OR b )", 1, 2), or(sql("a", 1), sql("b", 2)));
        assertEquals(sql("( a OR b OR c )", 1, 2, 3), or(sql("a", 1), sql("b"), sql("c", 2, 3)));
        assertEquals(sql("a", 1), or(sql("a", 1)));
    }
    @Test void testOrCollection() {
        assertEquals(sql("( a OR b )", 1, 2), or(listOf(sql("a", 1), sql("b", 2))));
        assertEquals(sql("( a OR b OR c )", 1, 2, 3), or(listOf(sql("a", 1), sql("b"), sql("c", 2, 3))));
        assertEquals(sql("a", 1), or(listOf(sql("a", 1))));
        assertEquals(sql("1<>2"), or(emptyList()));
    }
    @Test void testOrIterator() {
        assertEquals(sql("( a OR b )", 1, 2), or(listOf(sql("a", 1), sql("b", 2)).iterator()));
        assertEquals(sql("( a OR b OR c )", 1, 2, 3), or(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()));
        assertEquals(sql("a", 1), or(listOf(sql("a", 1)).iterator()));
        assertEquals(sql("1<>2"), or(emptyIterator()));
    }

    @Test void testWhereClause() {
        assertEquals(sql("WHERE a", 1), whereClause(sql("a", 1)));
        assertEquals(sql("WHERE a AND b", 1, 2), whereClause(sql("a", 1), sql("b", 2)));
        assertEquals(sql("WHERE a AND b AND c", 1, 2, 3), whereClause(sql("a", 1), sql("b"), sql("c", 2, 3)));
    }
    @Test void testWhereClauseCollection() {
        assertEquals(sql("WHERE a", 1), whereClause(listOf(sql("a", 1))));
        assertEquals(sql("WHERE a AND b", 1, 2), whereClause(listOf(sql("a", 1), sql("b", 2))));
        assertEquals(sql("WHERE a AND b AND c", 1, 2, 3), whereClause(listOf(sql("a", 1), sql("b"), sql("c", 2, 3))));

        assertEquals(EMPTY, whereClause(emptyList()));
    }
    @Test void testWhereClauseIterator() {
        assertEquals(sql("WHERE a", 1), whereClause(listOf(sql("a", 1)).iterator()));
        assertEquals(sql("WHERE a AND b", 1, 2), whereClause(listOf(sql("a", 1), sql("b", 2)).iterator()));
        assertEquals(sql("WHERE a AND b AND c", 1, 2, 3), whereClause(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()));

        assertEquals(EMPTY, whereClause(emptyIterator()));
    }

    //TODO variants taking streams where iterators are accepted
}

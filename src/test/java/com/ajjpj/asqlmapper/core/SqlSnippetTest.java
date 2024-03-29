package com.ajjpj.asqlmapper.core;

import static com.ajjpj.asqlmapper.testutil.CollectionUtils.listOf;
import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.*;

import com.ajjpj.acollections.AIterator;
import org.junit.jupiter.api.Test;
import static com.ajjpj.asqlmapper.core.SqlSnippet.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlSnippetTest {
    @Test void testEmpty() {
        final LocalDateTime ld = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
//        final LocalDateTime ld = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
        System.out.println(ld);
        System.out.println(ld.toInstant(ZoneOffset.UTC).getEpochSecond());


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
        assertEquals(sql("a b", 1, 2, 3), builder().append("a", 1, 2).append(sql("b", 3)).build());
        assertEquals(sql("ab", 1, 2, 3), builder().appendNoBlank("a", 1, 2).appendNoBlank(sql("b", 3)).build());
        assertEquals(sql("ab c", 1, 2, 3), builder().appendNoBlank("a", 1, 2).appendNoBlank(sql("b", 3)).append("c").build());

        SqlBuilder b = builder();
        assertEquals(EMPTY, b.build());
        assertEquals(sql("a"), b.append("a").build());
    }

    @Test void testBuilderAppendAll() {
        assertEquals(EMPTY, builder().appendAll(listOf()).build());
        assertEquals(sql("a", 1), builder().appendAll(listOf(sql("a", 1))).build());
        assertEquals(sql("a b c", 1, 2, 3), builder().appendAll(listOf(sql("a", 1), sql("b"), sql("c", 2, 3))).build());

        assertEquals(EMPTY, builder().appendAll(emptyIterator()).build());
        assertEquals(sql("a", 1), builder().appendAll(listOf(sql("a", 1)).iterator()).build());
        assertEquals(sql("a b c", 1, 2, 3), builder().appendAll(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()).build());
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
        assertEquals(sql("a * b", 1, 0, 2), combine(listOf(sql("a", 1), sql("b", 2)), sql("*", 0)));
        assertEquals(sql("a * b * c", 1, 0, 0, 2, 3), combine(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)), sql("*", 0)));

        assertEquals(sql("a", 1), combine(listOf(sql("a", 1)), sql("*", 0)));
        assertEquals(EMPTY, combine(listOf(), sql("*", 0)));
    }
    @Test void testCombinePrefixSuffix() {
        assertEquals(sql("< a * b >", 1, 0, 2), combine(listOf(sql("a", 1), sql("b", 2)), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("< a * b * c >", 1, 0, 0, 2, 3), combine(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)), sql("<"), sql("*", 0), sql(">")));

        assertEquals(sql("< a >", 1), combine(listOf(sql("a", 1)), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("< >"), combine(listOf(), sql("<"), sql("*", 0), sql(">")));
    }

    @Test void testCombineIterator() {
        assertEquals(sql("a * b", 1, 0, 2), combine(listOf(sql("a", 1), sql("b", 2)).iterator(), sql("*", 0)));
        assertEquals(sql("a * b * c", 1, 0, 0, 2, 3), combine(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator(), sql("*", 0)));

        assertEquals(sql("a", 1), combine(listOf(sql("a", 1)).iterator(), sql("*", 0)));
        assertEquals(EMPTY, combine(emptyIterator(), sql("*", 0)));
    }

    @Test void testCombineIteratorPrefixSuffix() {
        assertEquals(sql("< a * b >", 1, 0, 2), combine(listOf(sql("a", 1), sql("b", 2)).iterator(), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("< a * b * c >", 1, 0, 0, 2, 3), combine(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator(), sql("<"), sql("*", 0), sql(">")));

        assertEquals(sql("< a >", 1), combine(listOf(sql("a", 1)).iterator(), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("< >"), combine(emptyIterator(), sql("<"), sql("*", 0), sql(">")));
    }

    @Test void testCombineNoBlank() {
        assertEquals(sql("<a*b>", 1, 0, 2), combineNoBlank(listOf(sql("a", 1), sql("b", 2)), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("<a*b*c>", 1, 0, 0, 2, 3), combineNoBlank(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)), sql("<"), sql("*", 0), sql(">")));

        assertEquals(sql("<a>", 1), combineNoBlank(listOf(sql("a", 1)), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("<>"), combineNoBlank(listOf(), sql("<"), sql("*", 0), sql(">")));
    }

    @Test void testCombineNoBlankIterator() {
        assertEquals(sql("<a*b>", 1, 0, 2), combineNoBlank(listOf(sql("a", 1), sql("b", 2)).iterator(), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("<a*b*c>", 1, 0, 0, 2, 3), combineNoBlank(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator(), sql("<"), sql("*", 0), sql(">")));

        assertEquals(sql("<a>", 1), combineNoBlank(listOf(sql("a", 1)).iterator(), sql("<"), sql("*", 0), sql(">")));
        assertEquals(sql("<>"), combineNoBlank(emptyIterator(), sql("<"), sql("*", 0), sql(">")));
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
        assertEquals(sql("( x IN (?,?,?) OR x IN (?,?,?) OR x IN (?) )", 1, 2, 3, 4, 5, 6, 7), chunkedIn(sql("x"), listOf(1, 2, 3, 4, 5, 6, 7), 3));
        assertEquals(sql("( x IN (?,?) OR x IN (?,?) OR x IN (?,?) OR x IN (?) )", 1, 2, 3, 4, 5, 6, 7), chunkedIn(sql("x"), listOf(1, 2, 3, 4, 5, 6, 7), 2));

        assertEquals(sql("( y IN (?,?,?) OR y IN (?,?,?) )", 1, 2, 3, 4, 5, 6), chunkedIn(sql("y"), listOf(1, 2, 3, 4, 5, 6), 3));
        assertEquals(sql("( y IN (?,?) OR y IN (?,?) OR y IN (?,?) )", 1, 2, 3, 4, 5, 6), chunkedIn(sql("y"), listOf(1, 2, 3, 4, 5, 6), 2));

        assertEquals(sql("x IN (?,?,?)", 1, 2, 3), chunkedIn(sql("x"), listOf(1, 2, 3), 3));
        assertEquals(sql("x IN (?,?)", 1, 2), chunkedIn(sql("x"), listOf(1, 2), 3));
        assertEquals(sql("x IN (?,?)", 1, 2), chunkedIn(sql("x"), listOf(1, 2), 2));

        assertEquals(sql("x IN (?)", 1), chunkedIn(sql("x"), listOf(1), 2));

        assertEquals(FALSE, chunkedIn(sql("x"), listOf(), 2));
    }

    @Test void testChunkedInDefaultSize() {
        assertEquals(sql("x IN (?)", 1), chunkedIn(sql("x"), listOf(1)));
        assertEquals(FALSE, chunkedIn(sql("x"), listOf()));

        List<Integer> l = new ArrayList<>();
        for(int i=0; i<1000; i++) {
            l.add(i);
        }

        assertEquals(concat(sql("y"), in(l)), chunkedIn(sql("y"), l));

        final List<Integer> l2 = new ArrayList<>(l);
        l2.add(9999);
        assertEquals(
                concat(sql("( y"), in(l), sql("OR y IN (?) )", 9999)),
                chunkedIn(sql("y"), l2));
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
        assertEquals(TRUE, and(emptyList()));
    }
    @Test void testAndIterator() {
        assertEquals(sql("( a AND b )", 1, 2), and(listOf(sql("a", 1), sql("b", 2)).iterator()));
        assertEquals(sql("( a AND b AND c )", 1, 2, 3), and(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()));
        assertEquals(sql("a", 1), and(listOf(sql("a", 1)).iterator()));
        assertEquals(TRUE, and(emptyIterator()));
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
        assertEquals(TRUE, or(emptyList()));
    }
    @Test void testOrIterator() {
        assertEquals(sql("( a OR b )", 1, 2), or(listOf(sql("a", 1), sql("b", 2)).iterator()));
        assertEquals(sql("( a OR b OR c )", 1, 2, 3), or(listOf(sql("a", 1), sql("b"), sql("c", 2, 3)).iterator()));
        assertEquals(sql("a", 1), or(listOf(sql("a", 1)).iterator()));
        assertEquals(TRUE, or(emptyIterator()));
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

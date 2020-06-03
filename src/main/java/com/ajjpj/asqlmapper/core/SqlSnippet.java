package com.ajjpj.asqlmapper.core;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.ajjpj.acollections.AIterator;
import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.mutable.AMutableArrayWrapper;

/**
 * A SqlSnippet is some SQL code with its corresponding parameters.
 */
public class SqlSnippet {
    public static SqlSnippet EMPTY = new SqlSnippet("", AVector.empty());

    private final String sqlString;
    private final AVector<?> params;

    private SqlSnippet (String sqlString, AVector<?> params) {
        this.sqlString = sqlString;
        this.params = params;
    }

    public String getSql () {
        return sqlString;
    }

    public AList<?> getParams () {
        return params;
    }

    public static SqlSnippet sql(String sql, Object... params) {
        return new SqlSnippet(sql, AVector.from(params));
    }
    public static SqlSnippet sql(String sql, List<?> params) {
        return new SqlSnippet(sql, AVector.from(params));
    }

    public static SqlSnippet concat(SqlSnippet s0, SqlSnippet... snippets) {
        return concat(AIterator.single(s0).concat(AMutableArrayWrapper.from(snippets).iterator()));
    }
    public static SqlSnippet concat(Iterable<SqlSnippet> snippets) {
        return concat(snippets.iterator());
    }
    public static SqlSnippet concat(Iterator<SqlSnippet> snippets) {
        return builder().appendAll(snippets).build();
    }

    public static SqlBuilder builder() {
        return new SqlBuilder();
    }

    public static SqlSnippet param(Object value) {
        return sql("?", value);
    }

    public static SqlSnippet params(Iterable<?> values) {
        return params(values.iterator());
    }
    public static SqlSnippet params(Iterator<?> it) {
        return commaSeparated (AIterator.wrap(it).map(o -> sql("?", o)));
    }

    public static SqlSnippet commaSeparated (Iterable<SqlSnippet> coll) {
        return commaSeparated(coll.iterator());
    }
    public static SqlSnippet commaSeparated (Iterator<SqlSnippet> it) {
        return combine(it, sql(","));
    }

    public static SqlSnippet combine(Iterable<SqlSnippet> elements, SqlSnippet separator) {
        return combine(elements, EMPTY, separator, EMPTY);
    }
    public static SqlSnippet combine(Iterable<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        return combine(elements.iterator(), prefix, separator, suffix);
    }
    public static SqlSnippet combine(Iterator<SqlSnippet> elements, SqlSnippet separator) {
        return combine(elements, EMPTY, separator, EMPTY);
    }
    public static SqlSnippet combine(Iterator<SqlSnippet> elements, SqlSnippet prefix, SqlSnippet separator, SqlSnippet suffix) {
        final SqlBuilder result = builder();
        result.append(prefix);
        boolean first = true;
        while (elements.hasNext()) {
            final SqlSnippet s = elements.next();
            if (first) {
                result.append(s);
                first = false;
            }
            else {
                result.append(separator);
                result.append(s);
            }
        }
        result.append(suffix);
        return result.build();
    }

    public static SqlSnippet inSnippets(Iterable<SqlSnippet> elements) {
        return combine(elements, sql("IN ("), sql(","), sql(")"));
    }
    public static SqlSnippet in(Iterable<?> elements) {
        return inSnippets(AVector.of(params(elements)));
    }

    public static AList<SqlSnippet> chunkedIn(Iterable<?> elements) {
        return chunkedIn(elements, 1000);
    }
    public static AList<SqlSnippet> chunkedIn(Iterable<?> elements, int maxChunkSize) {
        final AVector.Builder<SqlSnippet> result = AVector.builder();

        List<?> elList = StreamSupport.stream(elements.spliterator(), false).collect(Collectors.toCollection(ArrayList::new));
        while(elList.size() > maxChunkSize) {
            final List<?> l = elList.subList(0, maxChunkSize);
            result.add(in(l));
            elList = elList.subList(maxChunkSize, elList.size());
        }

        if(elList.size() > 0) {
            result.add(in(elList));
        }

        return result.build();
    }

    public static SqlSnippet and(Iterable<SqlSnippet> elements) {
        return and(elements.iterator());
    }
    public static SqlSnippet and(SqlSnippet... elements) {
        return and(Arrays.asList(elements).iterator());
    }
    public static SqlSnippet and(Iterator<SqlSnippet> elements) {
        return combine(elements, sql("AND"));
    }

    public static SqlSnippet or(Iterable<SqlSnippet> elements) {
        return or(elements.iterator());
    }
    public static SqlSnippet or(SqlSnippet... elements) {
        return or(Arrays.asList(elements).iterator());
    }
    public static SqlSnippet or(Iterator<SqlSnippet> elements) {
        return combine(elements, sql("OR"));
    }

    public static SqlSnippet whereClause(Iterable<SqlSnippet> conditions) {
        return whereClause(conditions.iterator());
    }
    public static SqlSnippet whereClause(Iterator<SqlSnippet> conditions) {
        if(! conditions.hasNext()) return EMPTY;
        return combine(conditions, sql("WHERE"), sql("AND"), EMPTY);
    }

    @Override public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlSnippet that = (SqlSnippet) o;
        return Objects.equals(sqlString, that.sqlString) &&
                Objects.equals(params, that.params);
    }

    @Override public int hashCode () {
        return Objects.hash(sqlString, params);
    }

    @Override public String toString () {
        return "SqlSnippet{" +
                "sql='" + sqlString + '\'' +
                ", params=" + params +
                '}';
    }
}

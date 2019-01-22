package com.ajjpj.asqlmapper.core;

import com.ajjpj.acollections.AIterator;
import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.mutable.AMutableArrayWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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

    public static SqlSnippet concat(SqlSnippet... snippets) {
        return concat(AMutableArrayWrapper.wrap(snippets));
    }
    public static SqlSnippet concat(Iterable<SqlSnippet> snippets) {
        return concat(snippets.iterator());
    }
    public static SqlSnippet concat(Iterator<SqlSnippet> snippets) {
        if (!snippets.hasNext()) return EMPTY;

        final StringBuilder sql = new StringBuilder();
        final AVector.Builder<Object> params = AVector.builder();

        boolean first = true;
        while (snippets.hasNext()) {
            if(first) first = false;
            else sql.append(" ");

            final SqlSnippet s = snippets.next();
            sql.append(s.getSql());
            params.addAll(s.getParams());
        }

        return new SqlSnippet(sql.toString(), params.build());
    }

    public static SqlBuilder builder() {
        return new SqlBuilder();
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
        final SqlBuilder result = builder();
        boolean first = true;
        while (it.hasNext()) {
            final SqlSnippet s = it.next();
            if (first) {
                result.append(s);
                first = false;
            }
            else {
                result.append(",");
                result.append(s);
            }
        }
        return result.build();
    }

    //TODO in, and, or

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

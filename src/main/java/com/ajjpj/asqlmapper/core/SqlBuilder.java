package com.ajjpj.asqlmapper.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SqlBuilder {
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    public SqlBuilder append(SqlSnippet s) {
        if (sb.length() > 0) sb.append(" ");
        sb.append(s.getSql());
        this.params.addAll(s.getParams());
        return this;
    }

    public SqlBuilder append(String sql, Object... params) {
        return append(SqlSnippet.sql(sql, params));
    }

    public SqlBuilder appendAll(Iterable<SqlSnippet> it) {
        return appendAll(it.iterator());
    }
    public SqlBuilder appendAll(Iterator<SqlSnippet> it) {
        while (it.hasNext())
            append(it.next());
        return this;
    }

    public SqlSnippet build() {
        return SqlSnippet.sql(sb.toString(), params);
    }
}

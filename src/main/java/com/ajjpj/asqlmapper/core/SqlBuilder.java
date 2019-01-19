package com.ajjpj.asqlmapper.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SqlBuilder {
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    public SqlBuilder append(SqlSnippet s) {
        sb.append(s.getSql());
        this.params.addAll(s.getParams());
        return this;
    }

    public SqlBuilder append(String sql, Object... params) {
        sb.append(sql);
        this.params.addAll(Arrays.asList(params));
        return this;
    }

    public SqlSnippet build() {
        return SqlSnippet.sql(sb.toString(), params);
    }
}

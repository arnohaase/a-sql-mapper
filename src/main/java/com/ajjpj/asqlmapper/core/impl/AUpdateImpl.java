package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.asqlmapper.core.AUpdate;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.SqlSnippet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AUpdateImpl implements AUpdate {
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;

    public AUpdateImpl (SqlSnippet sql, PrimitiveTypeRegistry primTypes) {
        this.sql = sql;
        this.primTypes = primTypes;
    }

    @Override public int execute (Connection conn) throws SQLException {
        try (final PreparedStatement ps = conn.prepareStatement(sql.getSql())) {
            SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
            return ps.executeUpdate();
        }
    }
}

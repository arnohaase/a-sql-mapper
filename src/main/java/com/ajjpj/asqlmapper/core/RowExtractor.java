package com.ajjpj.asqlmapper.core;

import com.ajjpj.asqlmapper.core.impl.CanHandle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;


public interface RowExtractor extends CanHandle {
    boolean canHandle(Class<?> cls);

    default Object mementoPerQuery(Class<?> cls, PrimitiveTypeRegistry primTypes, ResultSet rs) throws SQLException {
        return null;
    }
    <T> T fromSql(Connection conn, Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException;
}

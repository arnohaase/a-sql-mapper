package com.ajjpj.asqlmapper.core;

import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.impl.CanHandle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;


public interface RowExtractor extends CanHandle {
    boolean canHandle(Class<?> cls);

    default Object mementoPerQuery(Class<?> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, boolean isStreaming) throws SQLException {
        return null;
    }

    <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, SqlRow row, Object mementoPerQuery, boolean isStreaming, Map<String,Object> injectedPropsValues) throws SQLException;
}

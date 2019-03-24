package com.ajjpj.asqlmapper.core;

import com.ajjpj.asqlmapper.core.impl.CanHandle;
import com.ajjpj.asqlmapper.core.provided.ProvidedProperties;

import java.sql.ResultSet;
import java.sql.SQLException;


public interface RowExtractor extends CanHandle {
    boolean canHandle(Class<?> cls);

    default Object mementoPerQuery(Class<?> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, boolean isStreaming) throws SQLException {
        return null;
    }

    <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, boolean isStreaming, ProvidedProperties providedProperties) throws SQLException;
}

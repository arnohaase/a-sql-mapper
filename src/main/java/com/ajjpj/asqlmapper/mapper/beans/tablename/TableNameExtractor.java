package com.ajjpj.asqlmapper.mapper.beans.tablename;

import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;

import java.sql.Connection;

public interface TableNameExtractor {
    String tableNameForBean(Connection conn, Class<?> beanType, SchemaRegistry schemaRegistry);
}

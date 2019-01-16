package com.ajjpj.asqlmapper.mapper.beans.tablename;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.annotations.Table;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;

import java.sql.Connection;

public class DefaultTableNameExtractor implements TableNameExtractor {
    @Override public String tableNameForBean (Connection conn, Class<?> beanType, SchemaRegistry schemaRegistry) {
        return AOption.of(beanType.getAnnotation(Table.class))
                .map(Table::value)
                .orElse(beanType.getSimpleName());
    }
}

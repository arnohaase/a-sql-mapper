package com.ajjpj.asqlmapper.mapper.beans.tablename;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.annotations.Table;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

import java.sql.Connection;

public class DefaultTableNameExtractor implements TableNameExtractor {
    @Override public String tableNameForBean (Connection conn, Class<?> beanType, SchemaRegistry schemaRegistry) {
        final ASet<String> tableNamesFromAnnotation = BeanReflectionHelper.allSuperTypes(beanType)
                .flatMap(c -> AOption.of(c.getAnnotation(Table.class)).map(Table::value))
                .toSet();

        switch(tableNamesFromAnnotation.size()) {
            case 0: return beanType.getSimpleName();
            case 1: return tableNamesFromAnnotation.head();
            default: throw new IllegalArgumentException("conflicting @Table annotations in inheritance hierarchy of " + beanType + ": " + tableNamesFromAnnotation);
        }
    }
}

package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.sql.Connection;

import com.ajjpj.asqlmapper.mapper.beans.BeanMapping;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;

public interface ManyToManyResolver {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    ManyToManySpec resolve(Connection conn, BeanMapping ownerMapping, String propertyName,
                           TableNameExtractor tableNameExtractor, SchemaRegistry schemaRegistry);
}

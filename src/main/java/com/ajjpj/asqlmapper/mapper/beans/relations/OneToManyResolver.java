package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.sql.Connection;
import java.util.Optional;

import com.ajjpj.asqlmapper.mapper.beans.BeanMapping;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;

public interface OneToManyResolver {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    OneToManySpec resolve(Connection conn, BeanMapping ownerMapping, String propertyName,
                                 TableNameExtractor tableNameExtractor, SchemaRegistry schemaRegistry,
                                 Optional<String> optDetailTable, Optional<String> optFk, Optional<String> optPk);
}

package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.sql.Connection;
import java.util.Optional;

import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.annotations.OneToMany;
import com.ajjpj.asqlmapper.mapper.beans.BeanMapping;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.ForeignKeySpec;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

public class DefaultOneToManyResolver implements OneToManyResolver {
    public OneToManySpec resolve(Connection conn, BeanMapping ownerMapping, String propertyName,
                                 TableNameExtractor tableNameExtractor, SchemaRegistry schemaRegistry) {
        final BeanProperty toManyProp = ownerMapping.beanMetaData().beanProperties().get(propertyName);
        if (toManyProp == null) {
            throw new IllegalArgumentException(ownerMapping.beanMetaData().beanType() + " has no mapped property " + propertyName);
        }

        final Optional<OneToMany> annot = toManyProp.getAnnotation(OneToMany.class);
        final Class<?> elementClass = elementClass(annot, toManyProp);
        final String elementTable = elementTable(annot, tableNameExtractor, conn, elementClass, schemaRegistry);
        final ForeignKeySpec fk = foreignKeySpec(annot, elementTable, ownerMapping, conn, schemaRegistry);

        final Class<?> keyType = ownerMapping.beanMetaData().getBeanPropertyForColumnName(fk.pkColumnName()).propClass();

        return new OneToManySpec(fk, elementClass, CollectionBuildStrategy.get(toManyProp.propClass()), keyType);
    }

    private Class<?> elementClass(Optional<OneToMany> annot, BeanProperty toManyProp) {
        if (annot.isPresent() && annot.get().elementType() != Void.class) {
            return annot.get().elementType();
        }
        return BeanReflectionHelper.elementType(toManyProp.propType());
    }

    private String elementTable(Optional<OneToMany> annot, TableNameExtractor tableNameExtractor, Connection conn, Class<?> beanType,
                                SchemaRegistry schemaRegistry) {
        if (annot.isPresent() && !annot.get().elementTable().isEmpty()) {
            return annot.get().elementTable();
        }

        return tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry);
    }

    private ForeignKeySpec foreignKeySpec(Optional<OneToMany> annot, String elementTable, BeanMapping ownerMapping, Connection conn,
                                          SchemaRegistry schemaRegistry) {
        if (annot.isPresent() && !annot.get().fkName().isEmpty()) {
            return new ForeignKeySpec(annot.get().fkName(), elementTable, ownerMapping.pkProperty().columnName(), ownerMapping.tableName());
        } else {
            final TableMetaData elementMetaData = schemaRegistry.getRequiredTableMetaData(conn, elementTable);
            return elementMetaData.uniqueFkTo(ownerMapping.tableName());
        }
    }
}

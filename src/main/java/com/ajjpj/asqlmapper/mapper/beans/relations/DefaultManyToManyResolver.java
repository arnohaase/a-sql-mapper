package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.sql.Connection;

import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.annotations.ManyToMany;
import com.ajjpj.asqlmapper.mapper.beans.BeanMapping;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

public class DefaultManyToManyResolver implements ManyToManyResolver {
    @Override public ManyToManySpec resolve(Connection conn, BeanMapping ownerMapping, String propertyName, TableNameExtractor tableNameExtractor,
                                            SchemaRegistry schemaRegistry) {
        final BeanProperty toManyProp = ownerMapping.beanMetaData().beanProperties().get(propertyName);
        if (toManyProp == null) {
            throw new IllegalArgumentException(ownerMapping.beanMetaData().beanType() + " has no mapped property " + propertyName);
        }

        final Class<?> detailElementClass = BeanReflectionHelper.elementType(toManyProp.propType());

        final ManyToMany manyToMany = toManyProp
                .getAnnotation(ManyToMany.class)
                .orElseThrow(() -> new IllegalArgumentException(
                        "property " + propertyName + " of class " + ownerMapping.beanMetaData().beanType().getName() + " has no @ManyToMany"));

        final TableMetaData manyManySchema = schemaRegistry
                .getTableMetaData(conn, manyToMany.manyManyTable())
                .orElseThrow(() -> new IllegalArgumentException("table " + manyToMany.manyManyTable() + " does not exist"));

        final String fkToOwner = manyToMany.fkToMaster().isEmpty() ?
                manyManySchema.uniqueFkTo(ownerMapping.tableName()).fkColumnName() :
                manyToMany.fkToMaster();

        final String detailTableName;
        if (manyToMany.detailTable().isEmpty()) {
            detailTableName = tableNameExtractor.tableNameForBean(conn, detailElementClass, schemaRegistry);
        } else {
            detailTableName = manyToMany.detailTable();
        }

        final String fkToCollection;
        if (manyToMany.fkToDetail().isEmpty()) {
            fkToCollection = manyManySchema.uniqueFkTo(detailTableName).fkColumnName();
        } else {
            fkToCollection = manyToMany.fkToDetail();
        }

        final String detailPk;
        if (manyToMany.detailPk().isEmpty()) {
            detailPk = schemaRegistry
                    .getRequiredTableMetaData(conn, detailTableName)
                    .getUniquePkColumn()
                    .colName();
        } else {
            detailPk = manyToMany.detailPk();
        }

        final Class<?> keyType = ownerMapping.pkProperty().propClass();

        return new ManyToManySpec(manyToMany.manyManyTable(), fkToOwner, fkToCollection,
                ownerMapping.pkProperty().columnName(), detailTableName, detailPk,
                detailElementClass, CollectionBuildStrategy.get(toManyProp.propClass()),
                keyType);
    }
}

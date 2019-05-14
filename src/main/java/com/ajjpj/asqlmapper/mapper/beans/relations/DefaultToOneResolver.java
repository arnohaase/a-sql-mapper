package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.sql.Connection;
import java.util.Optional;

import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.annotations.ManyToOne;
import com.ajjpj.asqlmapper.mapper.beans.BeanMapping;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.ForeignKeySpec;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;

public class DefaultToOneResolver implements ToOneResolver {
    @Override public ToOneSpec resolve(Connection conn, BeanMapping ownerMapping, String propertyName, TableNameExtractor tableNameExtractor,
                                       SchemaRegistry schemaRegistry) {
        final BeanProperty refProp = ownerMapping.beanMetaData().getRequiredProperty(propertyName);

        final Optional<ManyToOne> annot = refProp.getAnnotation(ManyToOne.class);

        final String referencedTable = referencedTable(annot, tableNameExtractor, conn, refProp, schemaRegistry);
        final ForeignKeySpec foreignKeySpec = foreignKeySpec(annot, ownerMapping, referencedTable, conn, schemaRegistry);

        final Class<?> keyType = schemaRegistry
                .getRequiredTableMetaData(conn, foreignKeySpec.pkTableName())
                .findColByName(foreignKeySpec.pkColumnName())
                .get()
                .colClass()
                .get();

        return new ToOneSpec(foreignKeySpec, refProp.propClass(), keyType);
    }

    private String referencedTable(Optional<ManyToOne> annot, TableNameExtractor tableNameExtractor,
                                   Connection conn, BeanProperty refProp, SchemaRegistry schemaRegistry) {
        if (annot.isPresent() && !annot.get().referencedTable().isEmpty()) {
            return annot.get().referencedTable();
        }
        return tableNameExtractor.tableNameForBean(conn, refProp.propClass(), schemaRegistry);
    }

    private ForeignKeySpec foreignKeySpec(Optional<ManyToOne> annot, BeanMapping ownerMapping, String referencedTable,
                                          Connection conn, SchemaRegistry schemaRegistry) {
        if (annot.isPresent() && !annot.get().fk().isEmpty()) {
            final String pk = annot.get().pk().isEmpty() ?
                    schemaRegistry.getRequiredTableMetaData(conn, referencedTable).getUniquePkColumn().colName() :
                    annot.get().pk();

            return new ForeignKeySpec(annot.get().fk(), ownerMapping.tableName(), pk, referencedTable);
        } else {
            return ownerMapping.tableMetaData().uniqueFkTo(referencedTable);
        }
    }
}

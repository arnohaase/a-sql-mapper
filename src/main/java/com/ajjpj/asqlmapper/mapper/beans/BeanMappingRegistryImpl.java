package com.ajjpj.asqlmapper.mapper.beans;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.javabeans.BeanMetaData;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistry;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

public class BeanMappingRegistryImpl implements BeanMappingRegistry {
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataRegistry metaDataRegistry;

    private final Map<Class<?>, BeanMapping> cache = new ConcurrentHashMap<>();

    public BeanMappingRegistryImpl (SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider, BeanMetaDataRegistry metaDataRegistry) {
        this.schemaRegistry = schemaRegistry;
        this.tableNameExtractor = tableNameExtractor;
        this.pkStrategyDecider = pkStrategyDecider;
        this.metaDataRegistry = metaDataRegistry;
    }

    @Override public BeanMetaDataRegistry metaDataRegistry () {
        return metaDataRegistry;
    }

    public void clearCache() {
        cache.clear();
    }

    @Override public boolean canHandle(Class<?> cls) {
        return metaDataRegistry.canHandle(cls);
    }

    @Override public BeanMapping getBeanMapping(Connection conn, Class<?> beanType) {
        return cache.computeIfAbsent(beanType, bt ->
                executeUnchecked(() -> {
                    final BeanMetaData beanMetaData = metaDataRegistry.getBeanMetaData(beanType);

                    final String tableName = tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry);
                    final AOption<TableMetaData> optTableMetaData = schemaRegistry.getTableMetaData(conn, tableName);
                    if (optTableMetaData.isEmpty())
                        throw new IllegalArgumentException(beanType + " is associated with table " + tableName + " which does not exist");

                    final TableMetaData tableMetaData = optTableMetaData.get();

                    final PkStrategy pkStrategy = pkStrategyDecider.pkStrategy(conn, beanType, tableMetaData);

                    final AMap<BeanProperty, ColumnMetaData> mappedProperties = beanMetaData
                            .beanProperties()
                            .values()
                            .flatMap(prop ->
                                    tableMetaData.findColByName(prop.columnName())
                                            .map(col -> new AbstractMap.SimpleImmutableEntry<>(prop, col))
                            )
                            .toMap();

                    return new BeanMapping(
                            beanMetaData,
                            tableMetaData,
                            pkStrategy,
                            mappedProperties);
                })
        );
    }
}

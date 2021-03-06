package com.ajjpj.asqlmapper.mapper.beans;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.javabeans.BeanMetaData;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistry;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.relations.*;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

public class BeanMappingRegistryImpl implements BeanMappingRegistry {
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataRegistry metaDataRegistry;

    private final OneToManyResolver oneToManyResolver;
    private final ManyToManyResolver manyToManyResolver;
    private final ToOneResolver toOneResolver;

    private final Map<Class<?>, BeanMapping> cache = new ConcurrentHashMap<>();
    private final Map<RelMapKey, OneToManySpec> oneToManyCache = new ConcurrentHashMap<>();
    private final Map<RelMapKey, ManyToManySpec> manyToManyCache = new ConcurrentHashMap<>();
    private final Map<RelMapKey, ToOneSpec> toOneCache = new ConcurrentHashMap<>();

    public BeanMappingRegistryImpl(SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider,
                                   BeanMetaDataRegistry metaDataRegistry, OneToManyResolver oneToManyResolver,
                                   ManyToManyResolver manyToManyResolver, ToOneResolver toOneResolver) {
        this.schemaRegistry = schemaRegistry;
        this.tableNameExtractor = tableNameExtractor;
        this.pkStrategyDecider = pkStrategyDecider;
        this.metaDataRegistry = metaDataRegistry;
        this.oneToManyResolver = oneToManyResolver;
        this.manyToManyResolver = manyToManyResolver;
        this.toOneResolver = toOneResolver;
    }

    @Override
    public BeanMetaDataRegistry metaDataRegistry() {
        return metaDataRegistry;
    }

    public void clearCache() {
        cache.clear();
    }

    @Override
    public boolean canHandle(Class<?> cls) {
        return metaDataRegistry.canHandle(cls);
    }

    @Override
    public BeanMapping getBeanMapping(Connection conn, Class<?> beanType) {
        return cache.computeIfAbsent(beanType, bt ->
                executeUnchecked(() -> {
                    final BeanMetaData beanMetaData = metaDataRegistry.getBeanMetaData(beanType);

                    final String tableName = tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry);

                    final TableMetaData tableMetaData = schemaRegistry
                            .getTableMetaData(conn, tableName)
                            .orElseThrow(() -> new IllegalArgumentException(beanType + " is associated with table " + tableName + " which does not exist"));

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
                            pkStrategy);
                })
        );
    }

    @Override
    public OneToManySpec resolveOneToMany(Connection conn, Class<?> ownerClass, String propertyName) {
        return oneToManyCache.computeIfAbsent(new RelMapKey(ownerClass, propertyName), k -> {
            final BeanMapping ownerMapping = getBeanMapping(conn, ownerClass);
            return oneToManyResolver.resolve(conn, ownerMapping, propertyName, tableNameExtractor, schemaRegistry);
        });
    }

    @Override
    public ManyToManySpec resolveManyToMany(Connection conn, Class<?> ownerClass, String propertyName) {
        return manyToManyCache.computeIfAbsent(new RelMapKey(ownerClass, propertyName), k -> {
            final BeanMapping ownerMapping = getBeanMapping(conn, ownerClass);
            return manyToManyResolver.resolve(conn, ownerMapping, propertyName, tableNameExtractor, schemaRegistry);
        });
    }

    @Override
    public ToOneSpec resolveToOne(Connection conn, Class owningClass, String propertyName) {
        return toOneCache.computeIfAbsent(new RelMapKey(owningClass, propertyName), k -> {
            final BeanMapping ownerMapping = getBeanMapping(conn, owningClass);
            return toOneResolver.resolve(conn, ownerMapping, propertyName, tableNameExtractor, schemaRegistry);
        });
    }
    private static class RelMapKey {
        final Class<?> ownerClass;
        final String propertyName;

        RelMapKey(Class<?> ownerClass, String propertyName) {
            this.ownerClass = ownerClass;
            this.propertyName = propertyName;
        }

        @Override
        public String toString() {
            return "FkMapKey{" +
                    "ownerClass=" + ownerClass +
                    ", propertyName='" + propertyName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RelMapKey fkMapKey = (RelMapKey) o;
            return Objects.equals(ownerClass, fkMapKey.ownerClass) &&
                    Objects.equals(propertyName, fkMapKey.propertyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ownerClass, propertyName);
        }
    }
}

package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.BeanMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.sql.Connection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


public class BeanMappingRegistryImpl implements BeanMappingRegistry {
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataExtractor beanMetaDataExtractor;

    private final Map<Class<?>, BeanMapping> queryMappingCache = new ConcurrentHashMap<>();
    private final Map<CacheKey, AOption<TableAwareBeanMetaData>> tableAwareCache = new ConcurrentHashMap<>();


    public BeanMappingRegistryImpl (SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider, BeanMetaDataExtractor beanMetaDataExtractor) {
        this.schemaRegistry = schemaRegistry;
        this.tableNameExtractor = tableNameExtractor;
        this.pkStrategyDecider = pkStrategyDecider;
        this.beanMetaDataExtractor = beanMetaDataExtractor;
    }

    public void clearCache() {
        queryMappingCache.clear();
        tableAwareCache.clear();
    }

    @Override public boolean canHandle (Class<?> cls) {
        return beanMetaDataExtractor.canHandle(cls);
    }


    @Override public QueryMappingBeanMetaData getQueryMappingMetaData (Connection conn, Class<?> beanType) {
        final QueryMappingBeanMetaData result = queryMappingCache.get(beanType);
        if (result != null)
            return result;
        return initMetaData(conn, beanType, AOption.empty());
    }

    @Override public TableAwareBeanMetaData getTableAwareMetaData (Connection conn, Class<?> beanType, AOption<String> providedTableName) {
        final AOption<TableAwareBeanMetaData> result = tableAwareCache.get(new CacheKey(beanType, providedTableName));
        if (result != null) {
            return result.orElseThrow(() -> new IllegalArgumentException("no corresponding table for bean " + beanType));
        }
        initMetaData(conn, beanType, providedTableName);
        return getTableAwareMetaData(null, beanType, providedTableName); // 'null' as a safe guard against endless recursion: should not happen anyway but still...
    }

    private BeanMapping initMetaData (Connection conn, Class<?> beanType, AOption<String> providedTableName) {
        return executeUnchecked(() -> {
            final String tableName = providedTableName.orElseGet(() -> tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry));
            final AOption<TableMetaData> optTableMetaData = schemaRegistry.getTableMetaData(conn, tableName);
            final AVector<BeanMappingProperty> beanProperties = beanMetaDataExtractor.beanProperties(conn, beanType, optTableMetaData);

            final PkStrategy pkStrategy = optTableMetaData.isDefined() ? pkStrategyDecider.pkStrategy(conn, beanType, optTableMetaData.get()) : null;

            final BeanMapping result = new BeanMapping(
                    beanType,
                    beanProperties,
                    optTableMetaData.orNull(),
                    pkStrategy,
                    beanMetaDataExtractor.builderFactoryFor(beanType),
                    beanMetaDataExtractor.builderFinalizerFor(beanType));

            queryMappingCache.put(beanType, result);
            if (optTableMetaData.isDefined()) {
                tableAwareCache.put(new CacheKey(beanType, providedTableName), AOption.of(result));
            }
            else {
                tableAwareCache.put(new CacheKey(beanType, providedTableName), AOption.empty());
            }
            return result;
        });
    }

    private static class CacheKey {
        private final Class<?> beanType;
        private final AOption<String> providedTableName;

        CacheKey (Class<?> beanType, AOption<String> providedTableName) {
            this.beanType = beanType;
            this.providedTableName = providedTableName;
        }

        @Override public boolean equals (Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return beanType.equals(cacheKey.beanType) &&
                    providedTableName.equals(cacheKey.providedTableName);
        }

        @Override public int hashCode () {
            return Objects.hash(beanType, providedTableName);
        }

        @Override public String toString () {
            return "CacheKey{" +
                    "beanType=" + beanType +
                    ", providedTableName=" + providedTableName +
                    '}';
        }
    }
}

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;
import static com.ajjpj.acollections.util.AUnchecker.executeUncheckedVoid;


public class BeanRegistryImpl implements BeanRegistry {
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataExtractor beanMetaDataExtractor;

    private final Map<Class<?>, BeanMetaData> queryMappingCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, AOption<TableAwareBeanMetaData>> tableAwareCache = new ConcurrentHashMap<>();

    public BeanRegistryImpl (SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider, BeanMetaDataExtractor beanMetaDataExtractor) {
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
        return initMetaData(conn, beanType);
    }

    @Override public TableAwareBeanMetaData getTableAwareMetaData (Connection conn, Class<?> beanType) {
        final AOption<TableAwareBeanMetaData> result = tableAwareCache.get(beanType);
        if (result != null) {
            return result.orElseThrow(() -> new IllegalArgumentException("no corresponding table for bean " + beanType));
        }
        initMetaData(conn, beanType);
        return getTableAwareMetaData(null, beanType); // 'null' as a safe guard against endless recursion: should not happen anyway but still...
    }

    private BeanMetaData initMetaData (Connection conn, Class<?> beanType) {
        return executeUnchecked(() -> {
            final AOption<TableMetaData> optTableMetaData = schemaRegistry.getTableMetaData(conn, tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry));
            final AVector<BeanProperty> beanProperties = beanMetaDataExtractor.beanProperties(conn, beanType, optTableMetaData);

            final PkStrategy pkStrategy = optTableMetaData.isDefined() ? pkStrategyDecider.pkStrategy(conn, beanType, optTableMetaData.get()) : null;

            final BeanMetaData result = new BeanMetaData(beanType,
                        beanProperties,
                        optTableMetaData.orNull(),
                        pkStrategy,
                        beanMetaDataExtractor.builderFactoryFor(beanType),
                        beanMetaDataExtractor.builderFinalizerFor(beanType));

            queryMappingCache.put(beanType, result);
            if (optTableMetaData.isDefined()) {
                tableAwareCache.put(beanType, AOption.of(result));
            }
            else {
                tableAwareCache.put(beanType, AOption.empty());
            }
            return result;
        });
    }
}

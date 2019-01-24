package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.immutable.AVector;
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


public class BeanRegistryImpl implements BeanRegistry {
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataExtractor beanMetaDataExtractor;

    private final Map<Class<?>, BeanMetaData> cache = new ConcurrentHashMap<>();

    public BeanRegistryImpl (SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider, BeanMetaDataExtractor beanMetaDataExtractor) {
        this.schemaRegistry = schemaRegistry;
        this.tableNameExtractor = tableNameExtractor;
        this.pkStrategyDecider = pkStrategyDecider;
        this.beanMetaDataExtractor = beanMetaDataExtractor;
    }

    public void clearCache() {
        cache.clear();
    }

    @Override public boolean canHandle (Class<?> cls) {
        return beanMetaDataExtractor.canHandle(cls);
    }

    @Override public BeanMetaData getMetaData (Connection conn, Class<?> beanType) {
        return cache.computeIfAbsent(beanType, bt -> executeUnchecked(() -> {
            final TableMetaData tableMetaData = schemaRegistry.getTableMetaData(conn, tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry));
            final PkStrategy pkStrategy = pkStrategyDecider.pkStrategy(conn, beanType, tableMetaData);
            final List<BeanProperty> beanProperties = beanMetaDataExtractor.beanProperties(conn, beanType, tableMetaData);

            return new BeanMetaData(beanType,
                    AVector.from(beanProperties),
                    tableMetaData,
                    pkStrategy,
                    beanMetaDataExtractor.builderFactoryFor(beanType),
                    beanMetaDataExtractor.builderFinalizerFor(beanType));
        }));
    }
}

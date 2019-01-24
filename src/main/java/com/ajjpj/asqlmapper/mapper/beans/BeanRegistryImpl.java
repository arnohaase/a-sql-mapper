package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.impl.SqlHelper;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.BeanMetaDataExtractor;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategyDecider;
import com.ajjpj.asqlmapper.mapper.beans.tablename.TableNameExtractor;
import com.ajjpj.asqlmapper.mapper.schema.SchemaRegistry;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


public class BeanRegistryImpl implements BeanRegistry {
    private final DataSource ds;
    private final SchemaRegistry schemaRegistry;
    private final TableNameExtractor tableNameExtractor;
    private final PkStrategyDecider pkStrategyDecider;
    private final BeanMetaDataExtractor beanMetaDataExtractor;

    private final Map<Class<?>, BeanMetaData> cache = new ConcurrentHashMap<>();

    public BeanRegistryImpl (DataSource ds, SchemaRegistry schemaRegistry, TableNameExtractor tableNameExtractor, PkStrategyDecider pkStrategyDecider, BeanMetaDataExtractor beanMetaDataExtractor) {
        this.ds = ds;
        this.schemaRegistry = schemaRegistry;
        this.tableNameExtractor = tableNameExtractor;
        this.pkStrategyDecider = pkStrategyDecider;
        this.beanMetaDataExtractor = beanMetaDataExtractor;
    }

    public void clearCache() {
        cache.clear();
    }


    @Override public BeanMetaData getMetaData (Class<?> beanType) {
        return cache.computeIfAbsent(beanType, bt -> executeUnchecked(() -> {
            final Connection conn = ds.getConnection();
            try {
                final TableMetaData tableMetaData = schemaRegistry.getTableMetaData(conn, tableNameExtractor.tableNameForBean(conn, beanType, schemaRegistry));
                final PkStrategy pkStrategy = pkStrategyDecider.pkStrategy(conn, beanType, tableMetaData);
                final List<BeanProperty> beanProperties = beanMetaDataExtractor.beanProperties(conn, beanType, tableMetaData);

                return new BeanMetaData(beanType,
                        AVector.from(beanProperties),
                        tableMetaData,
                        pkStrategy,
                        beanMetaDataExtractor.builderFactoryFor(beanType),
                        beanMetaDataExtractor.builderFinalizerFor(beanType));
            }
            finally {
                SqlHelper.closeQuietly(conn);
            }
        }));
    }
}

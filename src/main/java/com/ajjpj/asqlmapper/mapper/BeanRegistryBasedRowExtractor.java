package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.mapper.beans.BeanMetaData;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


class BeanRegistryBasedRowExtractor implements RowExtractor {
    private final DataSource ds;
    private final BeanRegistry beanRegistry;

    public BeanRegistryBasedRowExtractor (DataSource ds, BeanRegistry beanRegistry) {
        this.ds = ds;
        this.beanRegistry = beanRegistry;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return getMetaData(cls) != null;
    }

    private BeanMetaData getMetaData(Class<?> beanType) {
        return executeUnchecked(() -> {
            final Connection conn = ds.getConnection();
            //noinspection TryFinallyCanBeTryWithResources
            try {
                return beanRegistry.getMetaData(conn, beanType);
            }
            catch (Exception exc) {
                //TODO better error logging (e.g. 'table ... not found')
                return null;
            }
            finally {
                conn.close();
            }
        });
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException {
        return fromSql(cls, primTypes, rs, mementoPerQuery, ProvidedValues.empty());
    }

    public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, ProvidedValues providedValues) throws SQLException {
        final BeanMetaData beanMetaData = getMetaData(cls);
        if (beanMetaData.tableMetaData().pkColumns().size() != 1)
            throw new IllegalArgumentException("bean must have exactly one PK column for provided values to work - table " + beanMetaData.tableMetaData() + " has PK columns " + beanMetaData.tableMetaData());
        final String pkColumnName = beanMetaData.tableMetaData().pkColumns().head().colName;

        Object builder = beanMetaData.newBuilder();
        for(BeanProperty prop: beanMetaData.beanProperties()) {
            final AOption<Map<?,?>> optProvided = providedValues.getOptional(prop.name().toLowerCase());
            if(optProvided.isDefined() && optProvided.get().size() > 0) {
                final Map<?,?> provided = optProvided.get();
                final Class<?> keyClass = provided.keySet().iterator().next().getClass();

                final Object pk = primTypes.fromSql(keyClass, rs.getObject(pkColumnName));
                final Object value = provided.get(pk);
                builder = prop.setOnBuilder(builder, value);
            }
            else {
                final Object value = primTypes.fromSql(prop.propType(), rs.getObject(prop.columnMetaData().colName));
                builder = prop.setOnBuilder(builder, value);
            }
        }
        //noinspection unchecked
        return (T) beanMetaData.finalizeBuilding(builder);
    }
}

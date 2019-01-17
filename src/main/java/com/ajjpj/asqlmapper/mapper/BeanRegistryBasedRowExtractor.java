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
                return null;
            }
            finally {
                conn.close();
            }
        });
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException {
        return fromSql(cls, primTypes, rs, mementoPerQuery, AMap.empty());
    }

    public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, AMap<String, Map<Object,Object>> providedValues) throws SQLException {
        final BeanMetaData beanMetaData = getMetaData(cls);

        Object builder = beanMetaData.newBuilder();
        for(BeanProperty prop: beanMetaData.beanProperties()) {
            final AOption<Map<Object,Object>> provided = providedValues.getOptional(prop.name().toLowerCase());
            if(provided.isDefined()) {
                final BeanProperty pkProperty = beanMetaData.pkProperty();
                final Object pk = primTypes.fromSql(pkProperty.propType(), rs.getObject(beanMetaData.pkProperty().columnMetaData().colName));
                final Object value = provided.get().get(pk);
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

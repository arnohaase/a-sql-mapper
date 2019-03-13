package com.ajjpj.asqlmapper.javabeans;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistry;
import com.ajjpj.asqlmapper.mapper.beans.QueryMappingBeanMetaData;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedProperties;


class BeanRegistryBasedRowExtractor implements RowExtractor {
    private final BeanRegistry beanRegistry;

    public BeanRegistryBasedRowExtractor (BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return beanRegistry.canHandle(cls);
    }

    @Override public <T> T fromSql (Connection conn, Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException {
        return fromSql(conn, cls, primTypes, rs, mementoPerQuery, ProvidedProperties.empty());
    }

    public <T> T fromSql (Connection conn, Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, ProvidedProperties providedProperties) throws SQLException {
        final QueryMappingBeanMetaData beanMetaData = beanRegistry.getQueryMappingMetaData(conn, cls);

        final String pkColumnName;
        if (providedProperties.nonEmpty()) {
            pkColumnName = beanMetaData.pkColumnName().orElseThrow(() -> new IllegalArgumentException("bean " + cls.getName() + " must have exactly one known primary key column for providedProperties to work"));
        }
        else {
            pkColumnName = null;
        }

        Object builder = beanMetaData.newBuilder();
        for(BeanProperty prop: beanMetaData.beanProperties()) {
            if (providedProperties.hasValuesFor(prop.name())) {
                final Class<?> keyClass = providedProperties.pkType(prop.name());

                final Object pk = primTypes.fromSql(keyClass, rs.getObject(pkColumnName));
                final AOption<Object> optValue = providedProperties.get(prop.name(), pk);
                if (optValue.isDefined()) {
                    builder = prop.setOnBuilder(builder, optValue.get());
               }
            }
            else {
                final Object value = primTypes.fromSql(prop.propType(), rs.getObject(prop.columnName()));
                builder = prop.setOnBuilder(builder, value);
            }
        }
        //noinspection unchecked
        return (T) beanMetaData.finalizeBuilding(builder);
    }
}

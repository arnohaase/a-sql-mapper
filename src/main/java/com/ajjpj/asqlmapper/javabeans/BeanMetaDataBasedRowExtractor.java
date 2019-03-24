package com.ajjpj.asqlmapper.javabeans;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.provided.ProvidedProperties;

class BeanMetaDataBasedRowExtractor implements RowExtractor {
    private final BeanMetaDataRegistry beanRegistry;

    public BeanMetaDataBasedRowExtractor (BeanMetaDataRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return beanRegistry.canHandle(cls);
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, boolean isStreaming,
                                    ProvidedProperties providedProperties) throws SQLException {
        final BeanMetaData beanMetaData = beanRegistry.getBeanMetaData(cls);

        Object builder = beanMetaData.newBuilder();
        for(BeanProperty prop: beanMetaData.beanProperties().values()) {
            if (providedProperties.hasValuesFor(prop.name())) {
                final Class<?> keyClass = providedProperties.pkType(prop.name());
                final String referencedColumnName = providedProperties.referencedColumnNameFor(prop.name());

                final Object pk = primTypes.fromSql(keyClass, rs.getObject(referencedColumnName));
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
        return (T) beanMetaData.finalizeBuilder(builder);
    }
}

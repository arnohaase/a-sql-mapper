package com.ajjpj.asqlmapper.javabeans;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;

class BeanMetaDataBasedRowExtractor implements RowExtractor {
    private final BeanMetaDataRegistry beanRegistry;

    public BeanMetaDataBasedRowExtractor (BeanMetaDataRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return beanRegistry.canHandle(cls);
    }

    @Override public <T> T fromSql (Connection conn, Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException {
        final BeanMetaData beanMetaData = beanRegistry.getBeanMetaData(cls);

        Object builder = beanMetaData.newBuilder();
        for(BeanProperty prop: beanMetaData.beanProperties()) {
            final Object value = primTypes.fromSql(prop.propType(), rs.getObject(prop.columnName()));
            builder = prop.setOnBuilder(builder, value);
        }
        //noinspection unchecked
        return (T) beanMetaData.finalizeBuilder(builder);
    }
}

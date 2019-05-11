package com.ajjpj.asqlmapper.javabeans;

import java.util.Map;

import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.common.SqlRow;

class BeanMetaDataBasedRowExtractor implements RowExtractor {
    private final BeanMetaDataRegistry beanRegistry;

    public BeanMetaDataBasedRowExtractor (BeanMetaDataRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return beanRegistry.canHandle(cls);
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, SqlRow row, Object mementoPerQuery, boolean isStreaming,
                                    Map<String,Object> injectedPropsValues) {
        final BeanMetaData beanMetaData = beanRegistry.getBeanMetaData(cls);

        Object builder = beanMetaData.newBuilder();
        for(String injectedPropName: injectedPropsValues.keySet()) {
            builder = beanMetaData.beanProperties().get(injectedPropName).setOnBuilder(builder, injectedPropsValues.get(injectedPropName));
            //TODO better reporting for "not found"
        }

        for(String colName: row.columnNames()) {
            final BeanProperty prop = beanMetaData.getBeanPropertyForColumnName(colName);
            if(prop == null)
                continue;
            if(injectedPropsValues.containsKey(prop.name()))
                continue;

            builder = prop.setOnBuilder(builder, row.get(prop.propClass(), colName));
        }
        //noinspection unchecked
        return (T) beanMetaData.finalizeBuilder(builder);
    }
}

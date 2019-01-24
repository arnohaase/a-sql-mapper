package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.mapper.beans.BeanMetaData;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.BeanRegistry;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedProperties;

import java.sql.ResultSet;
import java.sql.SQLException;


class BeanRegistryBasedRowExtractor implements RowExtractor {
    private final BeanRegistry beanRegistry;

    public BeanRegistryBasedRowExtractor (BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return getMetaData(cls) != null;
    }

    private BeanMetaData getMetaData(Class<?> beanType) {
        try {
            return beanRegistry.getMetaData(beanType);
        }
        catch (Exception exc) {
            //TODO better error logging (e.g. 'table ... not found')
            return null;
        }
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException {
        return fromSql(cls, primTypes, rs, mementoPerQuery, ProvidedProperties.empty());
    }

    public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, ProvidedProperties providedProperties) throws SQLException {
        final BeanMetaData beanMetaData = getMetaData(cls);
        if (beanMetaData.tableMetaData().pkColumns().size() != 1)
            throw new IllegalArgumentException("bean must have exactly one PK column for provided values to work - table " + beanMetaData.tableMetaData() + " has PK columns " + beanMetaData.tableMetaData());
        final String pkColumnName = beanMetaData.tableMetaData().pkColumns().head().colName; //TODO pass this in as an optional value? any column 'referenced' by a many side would suffice...

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
                final Object value = primTypes.fromSql(prop.propType(), rs.getObject(prop.columnMetaData().colName));
                builder = prop.setOnBuilder(builder, value);
            }
        }
        //noinspection unchecked
        return (T) beanMetaData.finalizeBuilding(builder);
    }
}

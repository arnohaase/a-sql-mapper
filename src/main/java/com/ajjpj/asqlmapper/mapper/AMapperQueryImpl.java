package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.impl.AQueryImpl;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedProperties;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedValues;

import java.sql.ResultSet;
import java.sql.SQLException;


class AMapperQueryImpl<T> extends AQueryImpl<T> implements AMapperQuery<T> {
    private final BeanRegistryBasedRowExtractor beanExtractor;
    private final ProvidedProperties providedValues;

    AMapperQueryImpl (Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, BeanRegistryBasedRowExtractor beanExtractor,
                      ProvidedProperties providedValues, AVector<SqlEngineEventListener> listeners) {
        super(cls, sql, primTypes, beanExtractor, listeners);
        this.beanExtractor = beanExtractor;
        this.providedValues = providedValues;
    }

    @Override protected T doExtract (ResultSet rs, Object memento) throws SQLException {
        return beanExtractor.fromSql(rowClass, primTypes, rs, memento, providedValues);
    }

    @Override public AMapperQuery<T> withPropertyValues(String propName, ProvidedValues providedValues) {
        return new AMapperQueryImpl<>(rowClass, sql, primTypes, beanExtractor, this.providedValues.with(propName, providedValues), listeners);
    }
}

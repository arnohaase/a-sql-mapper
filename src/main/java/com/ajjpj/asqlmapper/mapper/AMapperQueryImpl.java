package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.acollections.AMap;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.impl.AQueryImpl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;


class AMapperQueryImpl<T> extends AQueryImpl<T> implements AMapperQuery<T> {
    private final BeanRegistryBasedRowExtractor beanExtractor;
    private final AMap<String, Map<?,?>> providedProperties;

    AMapperQueryImpl (Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, BeanRegistryBasedRowExtractor beanExtractor, AMap<String, Map<?,?>> providedProperties) {
        super(cls, sql, primTypes, beanExtractor);
        this.beanExtractor = beanExtractor;
        this.providedProperties = providedProperties;
    }

    @Override protected T doExtract (ResultSet rs, Object memento) throws SQLException {
        return beanExtractor.fromSql(cls, primTypes, rs, memento, providedProperties);
    }

    @Override public AMapperQuery<T> withPropertyValues(String propName, Map<?,?> providedValues) {
        return new AMapperQueryImpl<>(cls, sql, primTypes, beanExtractor, providedProperties.plus(propName.toLowerCase(), providedValues));
    }
}

package com.ajjpj.asqlmapper.mapper;

import java.sql.Connection;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.impl.AQueryImpl;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

public class AMapperQueryImpl<T> extends AQueryImpl<T> implements AMapperQuery<T> {
    private final SqlMapper mapper;

    public AMapperQueryImpl(SqlMapper mapper, Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes,
                            RowExtractor rowExtractor,
                            AVector<SqlEngineEventListener> listeners,
                            AOption<Supplier<Connection>> defaultConnectionSupplier,
                            AVector<InjectedProperty> injectedProperties) {
        super(cls, sql, primTypes, rowExtractor, listeners, defaultConnectionSupplier, injectedProperties);
        this.mapper = mapper;
    }
    @Override public AMapperQuery<T> withManyToMany(String propertyName) {
        return withInjectedProperty(mapper.manyToMany(propertyName));
    }
    @Override public AMapperQuery<T> withOneToMany(String propertyName) {
        return withInjectedProperty(mapper.oneToMany(propertyName));
    }
    @Override public AMapperQuery<T> withToOne(String propertyName) {
        return withInjectedProperty(mapper.toOne(propertyName));
    }

    @Override protected AQueryImpl<T> build(Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                                            AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier,
                                            AVector<InjectedProperty> injectedProperties) {
        return new AMapperQueryImpl<>(mapper, cls, sql, primTypes, rowExtractor, listeners, defaultConnectionSupplier, injectedProperties);
    }

    @Override public AMapperQuery<T> withInjectedProperty(InjectedProperty injectedProperty) {
        return (AMapperQuery<T>) super.withInjectedProperty(injectedProperty);
    }
}

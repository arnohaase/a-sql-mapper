package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;

public interface AMapperQuery<T> extends AQuery<T> {
    @Override AMapperQuery<T> withInjectedProperty(InjectedProperty injectedProperty);

    AMapperQuery<T> withManyToMany(String propertyName);
    AMapperQuery<T> withOneToMany(String propertyName);
    AMapperQuery<T> withToOne(String propertyName);
}

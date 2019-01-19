package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.asqlmapper.core.AQuery;

import java.util.Map;


public interface AMapperQuery<T> extends AQuery<T> {
    AMapperQuery<T> withPropertyValues (String propName, Map<?,?> providedValues);
}

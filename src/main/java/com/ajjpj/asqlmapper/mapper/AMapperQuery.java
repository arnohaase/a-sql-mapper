package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedProperties;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedValues;


public interface AMapperQuery<T> extends AQuery<T> {
    AMapperQuery<T> withPropertyValues (String propName, ProvidedValues providedValues);
}

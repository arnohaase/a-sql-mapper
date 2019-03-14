package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.asqlmapper.core.provided.ProvidedProperties;
import com.ajjpj.asqlmapper.core.provided.ProvidedValues;

import java.sql.Connection;


public interface ToManyQuery<K,R> {
    ProvidedValues execute();
    ProvidedValues execute(Connection conn);
    ToManyQuery withPropertyValues (String propertyName, ProvidedValues propertyValues);
    ToManyQuery withPropertyValues (ProvidedProperties providedProperties);
}

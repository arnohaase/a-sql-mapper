package com.ajjpj.asqlmapper.mapper;

import com.ajjpj.asqlmapper.mapper.provided.ProvidedProperties;
import com.ajjpj.asqlmapper.mapper.provided.ProvidedValues;

import java.sql.Connection;
import java.sql.SQLException;


public interface ToManyQuery<K,R> {
    ProvidedValues execute();
    ProvidedValues execute(Connection conn);
    ToManyQuery withPropertyValues (String propertyName, ProvidedValues propertyValues);
    ToManyQuery withPropertyValues (ProvidedProperties providedProperties);
}

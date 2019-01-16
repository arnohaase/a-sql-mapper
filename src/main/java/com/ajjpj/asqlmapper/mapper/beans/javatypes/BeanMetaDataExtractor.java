package com.ajjpj.asqlmapper.mapper.beans.javatypes;

import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.sql.Connection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;


public interface BeanMetaDataExtractor {
    List<BeanProperty> beanProperties(Connection conn, Class<?> beanType, TableMetaData tableMetaData, PrimitiveTypeRegistry primTypes);

    Supplier<Object> builderFactoryFor(Class<?> beanType);
    Function<Object,Object> builderFinalizerFor(Class<?> beanType);
}

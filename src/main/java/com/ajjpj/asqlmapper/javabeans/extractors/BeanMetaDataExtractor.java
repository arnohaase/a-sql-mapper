package com.ajjpj.asqlmapper.mapper.beans.javatypes;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.impl.CanHandle;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.sql.Connection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;


public interface BeanMetaDataExtractor extends CanHandle {
    AVector<BeanProperty> beanProperties(Connection conn, Class<?> beanType, AOption<TableMetaData> tableMetaData);

    Supplier<Object> builderFactoryFor(Class<?> beanType);
    Function<Object,Object> builderFinalizerFor(Class<?> beanType);
}

package com.ajjpj.asqlmapper.javabeans.extractors;

import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.impl.CanHandle;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;

public interface BeanMetaDataExtractor extends CanHandle {
    AVector<BeanProperty> beanProperties(Class<?> beanType);

    Supplier<Object> builderFactoryFor(Class<?> beanType);
    Function<Object,Object> builderFinalizerFor(Class<?> beanType);
}

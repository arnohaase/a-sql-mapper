package com.ajjpj.asqlmapper.javabeans;

import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;

public class BeanMetaData {
    private final Class<?> beanType;
    private final AVector<BeanProperty> beanProperties;

    private final Supplier<Object> builderFactory;
    private final Function<Object,Object> builderFinalizer;


    public BeanMetaData (Class<?> beanType, AVector<BeanProperty> beanProperties, Supplier<Object> builderFactory, Function<Object, Object> builderFinalizer) {
        this.beanType = beanType;
        this.beanProperties = beanProperties;
        this.builderFactory = builderFactory;
        this.builderFinalizer = builderFinalizer;
    }

    public Class<?> beanType () {
        return beanType;
    }

    public AVector<BeanProperty> beanProperties () {
        return beanProperties;
    }

    public Object newBuilder() {
        return builderFactory.get();
    }

    public Object finalizeBuilder(Object builder) {
        return builderFinalizer.apply(builder);
    }

    @Override
    public String toString () {
        return "BeanMetaData{" +
                "beanType=" + beanType +
                ", beanProperties=" + beanProperties +
                ", builderFactory=" + builderFactory +
                ", builderFinalizer=" + builderFinalizer +
                '}';
    }
}

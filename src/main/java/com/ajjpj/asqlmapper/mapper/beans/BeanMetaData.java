package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.util.function.Function;
import java.util.function.Supplier;


public class BeanMetaData {
    private final Class<?> beanType;
    private final AVector<BeanProperty> beanProperties;
    private final TableMetaData tableMetaData;
    private final PkStrategy pkStrategy;

    private final Supplier<Object> builderFactory;
    private final Function<Object,Object> builderFinalizer;

    private BeanProperty pkProperty;

    public BeanMetaData (Class<?> beanType, AVector<BeanProperty> beanProperties, TableMetaData tableMetaData, PkStrategy pkStrategy, Supplier<Object> builderFactory, Function<Object, Object> builderFinalizer) {
        this.beanType = beanType;
        this.beanProperties = beanProperties;
        this.tableMetaData = tableMetaData;
        this.pkStrategy = pkStrategy;
        this.builderFactory = builderFactory;
        this.builderFinalizer = builderFinalizer;
    }

    public Class<?> beanType() {
        return this.beanType;
    }

    public Object newBuilder() {
        return builderFactory.get();
    }
    public Object finalizeBuilding(Object builder) {
        return builderFinalizer.apply(builder);
    }

    public AVector<BeanProperty> beanProperties() {
        return this.beanProperties;
    }

    public TableMetaData tableMetaData() {
        return this.tableMetaData;
    }

    public PkStrategy pkStrategy() {
        return this.pkStrategy;
    }

    public BeanProperty pkProperty() {
        if (pkProperty == null) {
            final AVector<BeanProperty> pks = beanProperties().filter(BeanProperty::isPrimaryKey);
            if (pks.size() != 1) throw new IllegalStateException("exactly one PK column expected, was " + pks);
            pkProperty = pks.head();
        }
        return pkProperty;
    }
}

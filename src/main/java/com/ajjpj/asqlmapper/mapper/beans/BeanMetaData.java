package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
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

    private AOption<BeanProperty> pkProperty;
    private AVector<BeanProperty> insertedProperties;

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

    public AVector<BeanProperty> insertedBeanProperties() {
        if (insertedProperties == null) {
            insertedProperties = beanProperties
                    .filter(p -> p.columnMetaData() != null && (!p.columnMetaData().isPrimaryKey || !pkStrategy.isAutoIncrement()));
        }
        return insertedProperties;
    }

    public TableMetaData tableMetaData() {
        return this.tableMetaData;
    }

    public PkStrategy pkStrategy() {
        return this.pkStrategy;
    }

    public AOption<BeanProperty> pkProperty() {
        if (pkProperty == null) {
            final AVector<BeanProperty> pks = beanProperties().filter(BeanProperty::isPrimaryKey);
            switch(pks.size()) {
                case 0: pkProperty = AOption.empty(); break;
                case 1: pkProperty = AOption.of(pks.head()); break;
                default: throw new IllegalStateException("more than one PK column for bean " + beanType.getName() + ": " + pks);
            }
        }
        return pkProperty;
    }
}

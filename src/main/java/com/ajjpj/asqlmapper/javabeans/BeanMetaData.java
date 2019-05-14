package com.ajjpj.asqlmapper.javabeans;

import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.immutable.AVector;

public class BeanMetaData {
    private final Class<?> beanType;
    private final AMap<String, BeanProperty> beanProperties;
    private final AMap<String, BeanProperty> beanPropertiesByColumnName;

    private final Supplier<Object> builderFactory;
    private final Function<Object,Object> builderFinalizer;


    public BeanMetaData (Class<?> beanType, AVector<BeanProperty> beanProperties, Supplier<Object> builderFactory, Function<Object, Object> builderFinalizer) {
        this.beanType = beanType;
        this.beanProperties = beanProperties.groupBy(BeanProperty::name).mapValues(AVector::head);
        this.beanPropertiesByColumnName = beanProperties.groupBy(prop -> prop.columnName().toUpperCase()).mapValues(AVector::head);
        this.builderFactory = builderFactory;
        this.builderFinalizer = builderFinalizer;
    }

    public Class<?> beanType () {
        return beanType;
    }

    public AMap<String, BeanProperty> beanProperties () {
        return beanProperties;
    }
    public BeanProperty getBeanPropertyForColumnName (String columnName) {
        return beanPropertiesByColumnName.get(columnName.toUpperCase());
    }

    public BeanProperty getRequiredProperty(String propertyName) {
        return beanProperties().getOptional(propertyName)
                .orElseThrow(() -> new IllegalArgumentException(beanType() + " has no mapped property " + propertyName));
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

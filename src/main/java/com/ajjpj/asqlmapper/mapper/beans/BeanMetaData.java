package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.util.function.Function;
import java.util.function.Supplier;


public class BeanMetaData implements TableAwareBeanMetaData, QueryMappingBeanMetaData {
    private final Class<?> beanType;
    private final AVector<BeanProperty> beanProperties;
    private final TableMetaData tableMetaData;
    private final PkStrategy pkStrategy;

    private final Supplier<Object> builderFactory;
    private final Function<Object,Object> builderFinalizer;

    private AOption<BeanProperty> pkProperty;
    private final AVector<BeanProperty> writablePropertiesWithPk;
    private final AVector<BeanProperty> writablePropertiesWithoutPk;

    public BeanMetaData (Class<?> beanType, AVector<BeanProperty> beanProperties, TableMetaData tableMetaData, PkStrategy pkStrategy, Supplier<Object> builderFactory, Function<Object, Object> builderFinalizer) {
        this.beanType = beanType;
        this.beanProperties = beanProperties;
        this.tableMetaData = tableMetaData;
        this.pkStrategy = pkStrategy;
        this.builderFactory = builderFactory;
        this.builderFinalizer = builderFinalizer;

        if (tableMetaData != null) {
            this.writablePropertiesWithPk = beanProperties.filter(p -> p.columnMetaData().isDefined());
            this.writablePropertiesWithoutPk = writablePropertiesWithPk.filterNot(p -> p.columnMetaData().get().isPrimaryKey());
        }
        else {
            this.writablePropertiesWithPk = null;
            this.writablePropertiesWithoutPk = null;
        }
    }

    @Override public AOption<String> pkColumnName () {
        return pkProperty().map(BeanProperty::columnName);
    }

    @Override public String tableName () {
        return tableMetaData.tableName();
    }

    @Override public AList<BeanProperty> writableBeanProperties (boolean withPk) {
        return withPk ? writablePropertiesWithPk : writablePropertiesWithoutPk;
    }

    public Class<?> beanType() {
        return this.beanType;
    }

    @Override public Object newBuilder() {
        return builderFactory.get();
    }
    @Override public Object finalizeBuilding(Object builder) {
        return builderFinalizer.apply(builder);
    }

    @Override public AVector<BeanProperty> beanProperties() {
        return this.beanProperties;
    }

    @Override public PkStrategy pkStrategy() {
        return this.pkStrategy;
    }

    @Override public AOption<BeanProperty> pkProperty() {
        if (pkProperty == null) {
            final AVector<BeanProperty> pks = beanProperties().filter(p -> p.columnMetaData().exists(ColumnMetaData::isPrimaryKey));
            switch(pks.size()) {
                case 0: pkProperty = AOption.empty(); break;
                case 1: pkProperty = AOption.of(pks.head()); break;
                default: throw new IllegalStateException("more than one PK column for bean " + beanType.getName() + ": " + pks);
            }
        }
        return pkProperty;
    }
}

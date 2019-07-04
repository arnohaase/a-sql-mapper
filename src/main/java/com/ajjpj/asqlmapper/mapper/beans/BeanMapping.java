package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.ACollection;
import com.ajjpj.acollections.AList;
import com.ajjpj.asqlmapper.javabeans.BeanMetaData;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

public class BeanMapping {
    private final BeanMetaData beanMetaData;
    private final TableMetaData tableMetaData;
    private final PkStrategy pkStrategy;

    private final AList<String> mappedProperties;
    private final AList<String> mappedPropertiesWithoutPk;

    private BeanProperty pkProperty;

    public BeanMapping(BeanMetaData beanMetaData, TableMetaData tableMetaData, PkStrategy pkStrategy) {
        this.beanMetaData = beanMetaData;
        this.tableMetaData = tableMetaData;
        this.pkStrategy = pkStrategy;
        this.mappedProperties = beanMetaData.beanProperties().values()
                .filter(p -> tableMetaData.findColByName(p.columnName()).isDefined())
                .map(BeanProperty::name)
                .toVector()
                .sorted(); // sort to ensure a well-defined ordering across restarts / across JVMs

        final ACollection<String> pks = mappedProperties.filter(p -> tableMetaData.findColByName(beanProperty(p).columnName()).get().isPrimaryKey());
        switch (pks.size()) {
            case 0:
                throw new IllegalArgumentException("table " + tableName() + " associated with " + beanMetaData.beanType() + " has no mapped primary key");
            case 1:
                this.pkProperty = beanProperty(pks.head());
                break;
            default:
                throw new IllegalArgumentException(
                        "table " + tableName() + " associated with " + beanMetaData.beanType() + " has a primary key consisting of more than one column: " +
                                pks);
        }

        this.mappedPropertiesWithoutPk = mappedProperties.filterNot(p -> p.equals(pkProperty.name()));
    }

    public String tableName() {
        return tableMetaData.tableName();
    }

    public TableMetaData tableMetaData() {
        return tableMetaData;
    }

    public BeanMetaData beanMetaData() {
        return this.beanMetaData;
    }

    public PkStrategy pkStrategy() {
        return this.pkStrategy;
    }

    public AList<String> mappedProperties() {
        return mappedProperties;
    }
    public AList<String> mappedPropertiesWithoutPk() {
        return mappedPropertiesWithoutPk;
    }

    public BeanProperty beanProperty(String propertyName) {
        return beanMetaData.beanProperties().get(propertyName);
    }

    public BeanProperty pkProperty() {
        return pkProperty;
    }
}

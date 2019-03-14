package com.ajjpj.asqlmapper.mapper.beans;

import java.util.Map;

import com.ajjpj.acollections.ACollection;
import com.ajjpj.acollections.AMap;
import com.ajjpj.asqlmapper.javabeans.BeanMetaData;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;


public class BeanMapping {
    private final BeanMetaData beanMetaData;
    private final TableMetaData tableMetaData;
    private final PkStrategy pkStrategy;

    private final AMap<BeanProperty, ColumnMetaData> mappedProperties;
    private final AMap<BeanProperty, ColumnMetaData> mappedPropertiesWithoutPk;

    private BeanProperty pkProperty;

    public BeanMapping (BeanMetaData beanMetaData, TableMetaData tableMetaData, PkStrategy pkStrategy,
                        AMap<BeanProperty, ColumnMetaData> mappedProperties) {
        this.beanMetaData = beanMetaData;
        this.tableMetaData = tableMetaData;
        this.pkStrategy = pkStrategy;
        this.mappedProperties = mappedProperties;
        this.mappedPropertiesWithoutPk = mappedProperties.filterNot(e -> e.getValue().isPrimaryKey());

        final ACollection<BeanProperty> pks = mappedProperties
                .filter(p -> p.getValue().isPrimaryKey())
                .map(Map.Entry::getKey)
                .toVector();
        switch(pks.size()) {
            case 0:
                throw new IllegalArgumentException("table " + tableName() + " associated with " + beanMetaData.beanType() + " has no mapped primary key");
            case 1:
                this.pkProperty = pks.head();
                break;
            default:
                throw new IllegalArgumentException("table " + tableName() + " associated with " + beanMetaData.beanType() + " has a primary key consisting of more than one column: " + pks);
        }
    }

    public String tableName() {
        return tableMetaData.tableName();
    }

    public BeanMetaData beanMetaData() {
        return this.beanMetaData;
    }

    public PkStrategy pkStrategy() {
        return this.pkStrategy;
    }

    public AMap<BeanProperty, ColumnMetaData> mappedProperties () {
        return mappedProperties;
    }

    public AMap<BeanProperty, ColumnMetaData> mappedPropertiesWithoutPk () {
        return mappedPropertiesWithoutPk;
    }

    public BeanProperty pkProperty() {
        return pkProperty;
    }
}

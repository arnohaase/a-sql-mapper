package com.ajjpj.asqlmapper.mapper.beans;

import java.util.Map;

import com.ajjpj.acollections.ACollection;
import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;
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

    private AOption<BeanProperty> pkProperty;
//    private final AVector<BeanProperty> writablePropertiesWithPk;
//    private final AVector<BeanProperty> writablePropertiesWithoutPk;

    public BeanMapping (BeanMetaData beanMetaData, TableMetaData tableMetaData, PkStrategy pkStrategy,
                        AMap<BeanProperty, ColumnMetaData> mappedProperties) {
        this.beanMetaData = beanMetaData;
        this.tableMetaData = tableMetaData;
        this.pkStrategy = pkStrategy;
        this.mappedProperties = mappedProperties;
        this.mappedPropertiesWithoutPk = mappedProperties.filterNot(e -> e.getValue().isPrimaryKey());

//        if (tableMetaData != null) {
//            this.writablePropertiesWithPk = beanProperties.filter(p -> p.columnMetaData().isDefined());
//            this.writablePropertiesWithoutPk = writablePropertiesWithPk.filterNot(p -> p.columnMetaData().get().isPrimaryKey());
//        }
//        else {
            //TODO this should not be necessary any more
//
//            this.writablePropertiesWithPk = null;
//            this.writablePropertiesWithoutPk = null;
//        }
    }

    public AOption<String> pkColumnName() {
        return pkProperty().map(BeanProperty::columnName);
    }

    public String tableName() {
        return tableMetaData.tableName();
    }

//    public AList<BeanProperty> writableBeanProperties(boolean withPk) {
//        return withPk ? writablePropertiesWithPk : writablePropertiesWithoutPk;
//    }

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

    public AOption<BeanProperty> pkProperty() {
        if (pkProperty == null) {
            final ACollection<BeanProperty> pks = mappedProperties
                    .filter(p -> p.getValue().isPrimaryKey())
                    .map(Map.Entry::getKey)
                    .toVector();
            switch(pks.size()) {
                case 0: pkProperty = AOption.empty(); break;
                case 1: pkProperty = AOption.of(pks.head()); break;
                default: throw new IllegalStateException("more than one PK column for bean " + beanMetaData.beanType().getName() + ": " + pks);
            }
        }
        return pkProperty;
    }
}

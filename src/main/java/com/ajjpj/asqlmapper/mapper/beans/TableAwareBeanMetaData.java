package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.beans.primarykey.PkStrategy;

/**
 * This interface provides metadata that is necessary to build SQL statements. This
 *  includes a table name corresponding to a bean and table column types in addition
 *  to the bean's properties.
 */
public interface TableAwareBeanMetaData {
    PkStrategy pkStrategy();
    AOption<BeanProperty> pkProperty ();

    String tableName();
    AList<BeanProperty> writableBeanProperties(boolean withPk);
    AList<BeanProperty> beanProperties ();

}

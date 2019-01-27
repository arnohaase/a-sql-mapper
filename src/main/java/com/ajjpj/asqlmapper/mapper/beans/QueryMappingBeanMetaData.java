package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;

import java.util.List;


/**
 * This interface provides all that is needed to construct a bean instance from a ResultSet,
 *  i.e. basically a list of properties with corresponding column names, and a way to construct
 *  a new bean instance from their values.
 */
public interface QueryMappingBeanMetaData {
    Object newBuilder ();
    Object finalizeBuilding (Object builder);

    AList<BeanProperty> beanProperties ();

    AOption<String> pkColumnName();
}

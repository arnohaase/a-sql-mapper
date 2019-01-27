package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.asqlmapper.core.impl.CanHandle;

import java.sql.Connection;

public interface BeanRegistry extends CanHandle {
    QueryMappingBeanMetaData getQueryMappingMetaData(Connection conn, Class<?> beanType);
    TableAwareBeanMetaData getTableAwareMetaData(Connection conn, Class<?> beanType);
}

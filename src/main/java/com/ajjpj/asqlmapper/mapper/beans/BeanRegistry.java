package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.asqlmapper.core.impl.CanHandle;

import java.sql.Connection;

public interface BeanRegistry extends CanHandle {
    BeanMetaData getMetaData(Connection conn, Class<?> beanType);
}

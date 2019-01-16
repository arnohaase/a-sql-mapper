package com.ajjpj.asqlmapper.mapper.beans;

import java.sql.Connection;


public interface BeanRegistry {
    BeanMetaData getMetaData(Connection conn, Class<?> beanType);
}

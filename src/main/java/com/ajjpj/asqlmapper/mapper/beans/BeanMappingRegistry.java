package com.ajjpj.asqlmapper.mapper.beans;

import java.sql.Connection;

import com.ajjpj.asqlmapper.core.impl.CanHandle;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistry;

public interface BeanMappingRegistry extends CanHandle {
    BeanMetaDataRegistry metaDataRegistry();
    BeanMapping getBeanMapping(Connection conn, Class<?> beanType);
}

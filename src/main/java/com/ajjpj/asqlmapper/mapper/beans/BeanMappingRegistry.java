package com.ajjpj.asqlmapper.mapper.beans;

import java.sql.Connection;

import com.ajjpj.asqlmapper.core.impl.CanHandle;
import com.ajjpj.asqlmapper.javabeans.BeanMetaDataRegistry;
import com.ajjpj.asqlmapper.mapper.beans.relations.ManyToManySpec;
import com.ajjpj.asqlmapper.mapper.beans.relations.OneToManySpec;

public interface BeanMappingRegistry extends CanHandle {
    BeanMetaDataRegistry metaDataRegistry();
    BeanMapping getBeanMapping(Connection conn, Class<?> beanType);

    OneToManySpec resolveOneToMany(Connection conn, Class<?> ownerClass, String propertyName);
    ManyToManySpec resolveManyToMany(Connection conn, Class<?> ownerClass, String propertyName);
}

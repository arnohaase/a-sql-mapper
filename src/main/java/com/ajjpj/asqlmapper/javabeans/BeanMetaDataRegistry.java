package com.ajjpj.asqlmapper.javabeans;

import com.ajjpj.asqlmapper.core.impl.CanHandle;

public interface BeanMetaDataRegistry extends CanHandle {
    BeanMetaData getBeanMetaData(Class<?> beanType);
}

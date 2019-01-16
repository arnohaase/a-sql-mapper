package com.ajjpj.asqlmapper.core;

import com.ajjpj.asqlmapper.core.impl.CanHandle;

public interface PrimitiveTypeHandler extends CanHandle {
    <T> T fromSql(Class<T> targetType, Object o);
    Object toSql(Object o);
}

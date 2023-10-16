package com.ajjpj.asqlmapper.core;

import com.ajjpj.asqlmapper.core.impl.CanHandle;

/**
 * A primitive type handler that handles conversion of a group of Java application types to and from database column
 *  values. Instances need to be registered in a {@link PrimitiveTypeRegistry} in order to be called.<p>
 *
 * A {@code PrimitiveTypeHandler} is written from the perspective of one (or more) Java types: Its
 *  {@link #canHandle(Class)} method should return true if and only if it handles conversions from and to that class.
 *  {@link #toSql(Object)} is called for Java objects of one of those types, and {@link #fromSql(Class, Object)} is
 *  called for mapping a column to one of these types.<p>
 *
 * {@code null} values are never passed to {@code PrimitiveTypeHandler} methods, they are handled by calling code.
 */
public interface PrimitiveTypeHandler extends CanHandle {
    <T> T fromSql(Class<T> targetType, Object o);
    Object toSql(Object o);
}

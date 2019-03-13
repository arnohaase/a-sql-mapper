package com.ajjpj.asqlmapper.mapper.beans.javatypes;

import java.lang.reflect.Method;

/**
 * A ColumnNameExtractor
 */
public interface ColumnNameExtractor {
    String columnNameFor(Class<?> beanType, Method getter, String propertyName);
}

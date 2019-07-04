package com.ajjpj.asqlmapper.javabeans.columnnames;

import java.lang.reflect.Method;

/**
 * A ColumnNameExtractor
 */
public interface ColumnNameExtractor {
    String columnNameFor(Class<?> beanType, Method getter, String propertyName);
}

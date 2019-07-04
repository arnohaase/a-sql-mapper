package com.ajjpj.asqlmapper.javabeans.columnnames;

public class DirectColumnNameExtractor extends AnnotationBasedColumnNameExtractor {
    @Override protected String propertyNameToColumnName (Class<?> beanType, String propertyName) {
        return propertyName;
    }
}

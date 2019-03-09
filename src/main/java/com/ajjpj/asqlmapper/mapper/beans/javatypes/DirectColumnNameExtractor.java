package com.ajjpj.asqlmapper.mapper.beans.javatypes;


public class DirectColumnNameExtractor extends AnnotationBasedColumnNameExtractor {
    @Override protected String propertyNameToColumnName (Class<?> beanType, String propertyName) {
        return propertyName;
    }
}

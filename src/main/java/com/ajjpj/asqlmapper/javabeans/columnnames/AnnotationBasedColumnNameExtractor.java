package com.ajjpj.asqlmapper.javabeans.columnnames;

import java.lang.reflect.Method;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.annotations.Column;
import com.ajjpj.asqlmapper.mapper.beans.javatypes.ColumnNameExtractor;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

public abstract class AnnotationBasedColumnNameExtractor implements ColumnNameExtractor {
    @Override public String columnNameFor(Class<?> beanType, Method getter, String propertyName) {
        return columnNameFromAnnotation(beanType, getter).orElse(propertyName);
    }

    protected abstract String propertyNameToColumnName(Class<?> beanType, String propertyName);

    private AOption<String> columnNameFromAnnotation(Class<?> beanType, Method mtd) {
        final ASet<String> all = BeanReflectionHelper.allSuperMethods(beanType, mtd).flatMap(m -> AOption.of(m.getAnnotation(Column.class)).map(Column::value)).toSet();
        switch(all.size()) {
            case 0: return AOption.empty();
            case 1: return AOption.of(propertyNameToColumnName(beanType, all.head()));
            default: throw new IllegalArgumentException("there are conflicting @Column annotations on overridden methods of " + mtd + " in " + beanType);
        }
    }
}

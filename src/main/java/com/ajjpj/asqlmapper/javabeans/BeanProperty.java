package com.ajjpj.asqlmapper.javabeans;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Optional;

public class BeanProperty {
    private final Class<?> propClass;
    private final Type propType;
    private final String name;
    private final String columnName;

    private final Method getterMethod;
    private final Optional<Method> setterMethod;
    private final boolean setterReturnsBean;
    private final Optional<Field> field;

    private final Method builderSetterMethod;
    private final boolean builderSetterReturnsBean;

    public BeanProperty(Class<?> propClass, Type propType,
                        String name, String columnName, Method getterMethod, Optional<Method> setterMethod,
                        boolean setterReturnsBean,
                        Optional<Field> field,
                        Method builderSetterMethod, boolean builderSetterReturnsBean) {
        this.propClass = propClass;
        this.propType = propType;
        this.name = name;
        this.columnName = columnName;
        this.getterMethod = getterMethod;
        this.setterMethod = setterMethod;
        this.setterReturnsBean = setterReturnsBean;
        this.field = field;
        this.builderSetterMethod = builderSetterMethod;
        this.builderSetterReturnsBean = builderSetterReturnsBean;
    }

    public Class<?> propClass() {
        return propClass;
    }
    public Type propType() {
        return propType;
    }
    public String columnName() {
        return columnName;
    }

    public <T extends Annotation> Optional<T> getAnnotation(Class<T> annotationClass) {
        T mtdAnnotation = getterMethod.getAnnotation(annotationClass);
        if (mtdAnnotation != null || !field.isPresent()) {
            return Optional.ofNullable(mtdAnnotation);
        }

        return Optional.ofNullable(field.get().getAnnotation(annotationClass));
    }

    public Object get(Object bean) {
        return executeUnchecked(() -> getterMethod.invoke(bean));
    }

    public Object set(Object bean, Object value) {
        final Method mtd = setterMethod
                .orElseThrow(() -> new IllegalArgumentException("no setter for property " + name + " in bean " + getterMethod.getDeclaringClass().getName()));

        return executeUnchecked(() -> {
            final Object result = mtd.invoke(bean, value);
            return setterReturnsBean ? result : bean;
        });
    }

    public Object setOnBuilder(Object builder, Object value) {
        return executeUnchecked(() -> {
            final Object result = builderSetterMethod.invoke(builder, value);
            return builderSetterReturnsBean ? result : builder;
        });
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "BeanProperty{" +
                "propType=" + propType +
                ", name='" + name + '\'' +
                ", columnName=" + columnName +
                ", getterMethod=" + getterMethod +
                ", setterMethod=" + setterMethod +
                ", setterReturnsBean=" + setterReturnsBean +
                ", builderSetterMethod=" + builderSetterMethod +
                ", builderSetterReturnsBean=" + builderSetterReturnsBean +
                '}';
    }
}

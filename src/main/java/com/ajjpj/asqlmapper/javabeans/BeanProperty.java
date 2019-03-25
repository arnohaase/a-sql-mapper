package com.ajjpj.asqlmapper.javabeans;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.reflect.Method;

public class BeanProperty {
    private final Class<?> propType;
    private final String name;
    private final String columnName;

    private final Method getterMethod;
    private final Method setterMethod;
    private final boolean setterReturnsBean;

    private final Method builderSetterMethod;
    private final boolean builderSetterReturnsBean;

    public BeanProperty (Class<?> propType, String name, String columnName, Method getterMethod, Method setterMethod, boolean setterReturnsBean,
                         Method builderSetterMethod, boolean builderSetterReturnsBean) {
        this.propType = propType;
        this.name = name;
        this.columnName = columnName;
        this.getterMethod = getterMethod;
        this.setterMethod = setterMethod;
        this.setterReturnsBean = setterReturnsBean;
        this.builderSetterMethod = builderSetterMethod;
        this.builderSetterReturnsBean = builderSetterReturnsBean;
    }

    public Class<?> propType() {
        return propType;
    }
    public String columnName() {
        return columnName;
    }

    public Object get(Object bean) {
        return executeUnchecked(() -> getterMethod.invoke(bean));
    }

    public Object set(Object bean, Object value) {
        return executeUnchecked(() -> {
            final Object result = setterMethod.invoke(bean, value);
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
    public String toString () {
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

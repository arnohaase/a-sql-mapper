package com.ajjpj.asqlmapper.mapper.beans;

import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;

import java.lang.reflect.Method;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


public class BeanProperty {
    private final Class<?> propType;
    private final String name;
    private final ColumnMetaData columnMetaData;

    private final Method getterMethod;
    private final Method setterMethod;
    private final boolean setterReturnsBean;

    private final Method builderSetterMethod;

    public BeanProperty (Class<?> propType, String name, ColumnMetaData columnMetaData, Method getterMethod, Method setterMethod, boolean setterReturnsBean, Method builderSetterMethod) {
        this.propType = propType;
        this.name = name;
        this.columnMetaData = columnMetaData;
        this.getterMethod = getterMethod;
        this.setterMethod = setterMethod;
        this.setterReturnsBean = setterReturnsBean;
        this.builderSetterMethod = builderSetterMethod;
    }

    public boolean isPrimaryKey() {
        return columnMetaData != null && columnMetaData().isPrimaryKey;
    }

    public Class<?> propType() {
        return propType;
    }
    public ColumnMetaData columnMetaData() {
        if (columnMetaData == null)
            throw new IllegalArgumentException("no column meta data for property " + name + " of class " + getterMethod.getDeclaringClass());
        return columnMetaData;
    }

    public boolean isReadable() {
        return getterMethod != null;
    }
    public boolean isWritable() {
        return setterMethod != null;
    }

    public Object get(Object bean) {
        return executeUnchecked(() -> getterMethod.invoke(bean));
    }

    public Object set(Object bean, Object value) {
        return executeUnchecked(() -> {
            if (setterReturnsBean) {
                return setterMethod.invoke(bean, value);
            }
            else {
                setterMethod.invoke(bean, value);
                return bean;
            }
        });
    }

    public Object setOnBuilder(Object builder, Object value) {
        return executeUnchecked(() -> builderSetterMethod.invoke(builder, value));
    }

    public String name() {
        return name;
    }
}

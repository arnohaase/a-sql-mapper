package com.ajjpj.asqlmapper.javabeans;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Optional;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

public class BeanProperty {
    private final Class<?> beanClass;
    private final Class<?> settablePropClass;
    private final Type propType;
    private final String name;
    private final String columnName;

    private final Method getterMethod;
    private final Optional<Method> setterMethod;
    private final boolean setterReturnsBean;
    private final Optional<Field> field;

    private final Method builderSetterMethod;
    private final boolean builderSetterReturnsBean;

    public BeanProperty(Class<?> beanClass, Class<?> settablePropClass, Type propType,
                        String name, String columnName, Method getterMethod, Optional<Method> setterMethod,
                        boolean setterReturnsBean,
                        Optional<Field> field,
                        Method builderSetterMethod, boolean builderSetterReturnsBean) {
        this.beanClass = beanClass;
        this.settablePropClass = settablePropClass;
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
        return settablePropClass;
    }
    public Type propType() {
        return propType;
    }
    public String columnName() {
        return columnName;
    }

    public <T extends Annotation> Optional<T> getAnnotation(Class<T> annotationClass) {
        T mtdAnnotation = getterMethod.getAnnotation(annotationClass);
        if (mtdAnnotation != null) {
            return Optional.of(mtdAnnotation);
        }
        if (field.isPresent()) {
            T fieldAnnotation = field.get().getAnnotation(annotationClass);
            if (fieldAnnotation != null) {
                return Optional.of(fieldAnnotation);
            }
        }

        final ASet<T> superAnnotations = BeanReflectionHelper
                .allSuperMethods(beanClass, getterMethod)
                .flatMap(m -> AOption.of(m.getAnnotation(annotationClass)));

        switch (superAnnotations.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(superAnnotations.head());
            default:
                throw new IllegalStateException("no annotation " + annotationClass.getName() + " on method " + getterMethod +
                        " but on more than one super methods - this is not supported");
        }
    }

    public Object get(Object bean) {
        return executeUnchecked(() -> getterMethod.invoke(bean));
    }

    public Object set(Object bean, Object value) {
        final Method mtd = setterMethod
                .orElseThrow(() -> new IllegalStateException("no setter for property " + name + " in bean " + getterMethod.getDeclaringClass().getName()));

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

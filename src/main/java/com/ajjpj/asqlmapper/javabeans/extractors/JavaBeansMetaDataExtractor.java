package com.ajjpj.asqlmapper.javabeans.extractors;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.columnnames.ColumnNameExtractor;

public class JavaBeansMetaDataExtractor implements BeanMetaDataExtractor {
    private final ColumnNameExtractor columnNameExtractor;
    private final boolean requirePublicSetter;
    private final boolean requireVoidSetter;

    public JavaBeansMetaDataExtractor (ColumnNameExtractor columnNameExtractor) {
        this(columnNameExtractor, false, false);
    }

    public JavaBeansMetaDataExtractor (ColumnNameExtractor columnNameExtractor, boolean requirePublicSetter, boolean requireVoidSetter) {
        this.columnNameExtractor = columnNameExtractor;
        this.requirePublicSetter = requirePublicSetter;
        this.requireVoidSetter = requireVoidSetter;
    }

    @Override public boolean canHandle (Class<?> cls) {
        try {
            cls.getConstructor().newInstance();
            return true;
        }
        catch(Exception exc) {
            return false;
        }
    }

    @SuppressWarnings("CatchMayIgnoreException")
    private Method setterFor(Class<?> beanClass, Method getter) {
        final String setterName = "s" + getter.getName().substring(1);

        try {
            return beanClass.getMethod(setterName, getter.getReturnType());
        }
        catch(Exception exc) {
        }

        if(!requirePublicSetter) {
            try {
                final Method setter = getter.getDeclaringClass().getMethod(setterName, getter.getReturnType());
                setter.setAccessible(true);
                return setter;
            }
            catch(Exception exc) {
            }
        }
        return null;
    }

    private String propertyNameFor(Method getter) {
        final String raw = getter.getName().substring(3);
        if(raw.length() == 1)
            return raw.toLowerCase();
        if(Character.isUpperCase(raw.charAt(1))) // property name 'URL' for method getURL
            return raw;
        return Character.toLowerCase(raw.charAt(0)) + raw.substring(1);
    }

    @Override public AVector<BeanProperty> beanProperties (Class<?> beanType) {
        final AVector.Builder<BeanProperty> result = AVector.builder();

        Arrays.stream(beanType.getMethods())
                .filter(m -> m.getName().startsWith("get") && m.getParameterCount() == 0 && m.getName().length() > 3)
                .forEach(getter -> {
                    final Method setter = setterFor(beanType, getter);
                    if(setter == null)
                        return;
                    if(requireVoidSetter && setter.getReturnType() != Void.TYPE)
                        return;

                    final String propertyName = propertyNameFor(getter);
                    final String columnName = columnNameExtractor.columnNameFor(beanType, getter, propertyName);

                    result.add(new BeanProperty(getter.getReturnType(), getter.getGenericReturnType(), propertyName, columnName, getter, setter, false, setter, false));
                });

        return result.build();
    }

    @Override public Supplier<Object> builderFactoryFor (Class<?> beanType) {
        return () -> executeUnchecked(() -> beanType.getConstructor().newInstance());
    }

    @Override public Function<Object, Object> builderFinalizerFor (Class<?> beanType) {
        return x -> x;
    }
}

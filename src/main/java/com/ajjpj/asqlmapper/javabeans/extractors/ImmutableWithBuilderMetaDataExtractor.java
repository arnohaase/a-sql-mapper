package com.ajjpj.asqlmapper.javabeans.extractors;

import static com.ajjpj.acollections.mutable.AMutableArrayWrapper.wrap;
import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ajjpj.acollections.ACollection;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.columnnames.ColumnNameExtractor;
import com.ajjpj.asqlmapper.mapper.annotations.Ignore;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;

public class ImmutableWithBuilderMetaDataExtractor implements BeanMetaDataExtractor {
    private static final Logger log = LoggerFactory.getLogger(ImmutableWithBuilderMetaDataExtractor.class);

    private final ColumnNameExtractor columnNameExtractor;

    private final String builderFactoryName;
    private final String builderFinalizeMethodName;
    private final String setterPrefix;


    public ImmutableWithBuilderMetaDataExtractor (ColumnNameExtractor columnNameExtractor) {
        this(columnNameExtractor, "builder", "build", "with");
    }

    public ImmutableWithBuilderMetaDataExtractor (ColumnNameExtractor columnNameExtractor, String builderFactoryName, String builderFinalizeMethodName, String setterPrefix) {
        this.columnNameExtractor = columnNameExtractor;
        this.builderFactoryName = builderFactoryName;
        this.builderFinalizeMethodName = builderFinalizeMethodName;
        this.setterPrefix = setterPrefix;
    }

    private boolean hasIgnoreAnnotation(Class<?> beanType, Method mtd) {
        return BeanReflectionHelper.allSuperMethods(beanType, mtd).exists(g ->
                g.getAnnotation(Ignore.class) != null && g.getAnnotation(Ignore.class).value()
        );
    }

    @Override public boolean canHandle (Class<?> cls) {
        try {
            builderFactoryFor(cls);
            builderFinalizerFor(cls);
            return true;
        }
        catch (Exception exc) {
            return false;
        }
    }

    private Method setterFor(Class<?> type, String name, Class<?> getterType, Method getter) {
        final ACollection<Method> candidates = wrap(type.getMethods()).filter(m -> m.getName().equals(name) &&
                m.getParameterCount() == 1 &&
                m.getParameterTypes()[0].isAssignableFrom(getterType)
                );

        switch(candidates.size()) {
            case 0: throw new IllegalArgumentException("no corresponding setter '" + name + "' for " + getter + " in " + type);
            case 1: return candidates.iterator().next();
            default: throw new IllegalArgumentException("conflicting setters '" + name + "' for " + getter + " in " + type + ": " + candidates);
        }
    }

    @Override public AVector<BeanProperty> beanProperties (Class<?> beanType) {
        return executeUnchecked(() -> {
            final AVector<Method> getters = wrap(beanType.getMethods())
                    .filterNot(m -> m.getName().equals("hashCode") || m.getName().equals("toString") || m.getName().equals("getClass"))
                    .filterNot(m -> m.getReturnType() == void.class)
                    .filter(m -> m.getParameterCount() == 0 && (m.getModifiers() & Modifier.STATIC) == 0)
                    .toVector();

            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();

            return getters
                    .filterNot(g -> hasIgnoreAnnotation(beanType, g))
                    .map(getter -> executeUnchecked(() -> {
                        final Method setter = setterFor(beanType, setterPrefix + toFirstUpper(getter.getName()), getter.getReturnType(), getter);
                        final Method builderSetter = setterFor(builderClass, getter.getName(), getter.getReturnType(), getter);

                        final String columnName = columnNameExtractor.columnNameFor(beanType, getter, getter.getName());
                        return new BeanProperty(getter.getReturnType(), getter.getName(), columnName, getter, setter, true, builderSetter);
                    }));
        });
    }

    private String toFirstUpper(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    @Override public Supplier<Object> builderFactoryFor (Class<?> beanType) {
        return executeUnchecked(() -> {
            final Method mtd = beanType.getMethod(builderFactoryName);
            return () -> executeUnchecked(() -> mtd.invoke(null));
        });
    }

    @Override public Function<Object, Object> builderFinalizerFor (Class<?> beanType) {
        return executeUnchecked(() -> {
            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();
            final Method mtd = builderClass.getMethod(builderFinalizeMethodName);

            return builder -> executeUnchecked(() -> mtd.invoke(builder));
        });
    }
}

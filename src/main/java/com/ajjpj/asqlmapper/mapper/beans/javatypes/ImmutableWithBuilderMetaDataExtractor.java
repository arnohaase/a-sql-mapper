package com.ajjpj.asqlmapper.mapper.beans.javatypes;

import com.ajjpj.acollections.ACollection;
import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.annotations.Column;
import com.ajjpj.asqlmapper.mapper.annotations.Ignore;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.ajjpj.acollections.mutable.AMutableArrayWrapper.wrap;
import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


public class ImmutableWithBuilderMetaDataExtractor implements BeanMetaDataExtractor {
    private static final Logger log = LoggerFactory.getLogger(ImmutableWithBuilderMetaDataExtractor.class);

    private boolean hasIgnoreAnnotation(Class<?> beanType, Method mtd) {
        return BeanReflectionHelper.allSuperMethods(beanType, mtd).exists(g ->
                g.getAnnotation(Ignore.class) != null && g.getAnnotation(Ignore.class).value()
        );
    }

    private AOption<String> columnNameFromAnnotation(Class<?> beanType, Method mtd) {
        final ASet<String> all = BeanReflectionHelper.allSuperMethods(beanType, mtd).flatMap(m -> AOption.of(m.getAnnotation(Column.class)).map(Column::value)).toSet();
        switch(all.size()) {
            case 0: return AOption.empty();
            case 1: return AOption.of(all.head());
            default: throw new IllegalArgumentException("there are conflicting @Column annotations on overridden methods of " + mtd + " in " + beanType);
        }
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

    @Override public List<BeanProperty> beanProperties (Connection conn, Class<?> beanType, TableMetaData tableMetaData) {
        return executeUnchecked(() -> {
            final AVector<Method> getters = wrap(beanType.getMethods())
                    .filterNot(m -> m.getName().equals("hashCode") || m.getName().equals("toString") || m.getName().equals("getClass"))
                    .filterNot(m -> m.getReturnType() == void.class)
                    .filter(m -> m.getParameterCount() == 0 && (m.getModifiers() & Modifier.STATIC) == 0)
                    .toVector();

            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();

            return getters
                    .filterNot(g -> hasIgnoreAnnotation(beanType, g))
                    .flatMap(getter -> executeUnchecked(() -> {
                final Method setter = setterFor(beanType, "with" + toFirstUpper(getter.getName()), getter.getReturnType(), getter);
                final Method builderSetter = setterFor(builderClass, getter.getName(), getter.getReturnType(), getter);

                final String columnName = columnNameFromAnnotation(beanType, getter).orElse(getter.getName());
                final AOption<ColumnMetaData> optColumnMetaData = tableMetaData.findColByName(columnName);
                if (optColumnMetaData.isEmpty()) {
                    log.warn("no database column {}.{} for property {} of bean {}", tableMetaData.tableName, columnName, getter.getName(), beanType.getName());
                }

                final ColumnMetaData columnMetaData = optColumnMetaData.orNull();
                return AOption.some(new BeanProperty(getter.getReturnType(), getter.getName(), columnMetaData, getter, setter, true, builderSetter));
            }));
        });
    }

    private String toFirstUpper(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    @Override public Supplier<Object> builderFactoryFor (Class<?> beanType) {
        return executeUnchecked(() -> {
            final Method mtd = beanType.getMethod("builder");
            return () -> executeUnchecked(() -> mtd.invoke(null));
        });
    }

    @Override public Function<Object, Object> builderFinalizerFor (Class<?> beanType) {
        return executeUnchecked(() -> {
            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();
            final Method mtd = builderClass.getMethod("build");

            return builder -> executeUnchecked(() -> mtd.invoke(builder));
        });
    }
}

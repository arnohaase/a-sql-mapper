package com.ajjpj.asqlmapper.mapper.beans.javatypes;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.mapper.annotations.Column;
import com.ajjpj.asqlmapper.mapper.annotations.Ignore;
import com.ajjpj.asqlmapper.mapper.beans.BeanProperty;
import com.ajjpj.asqlmapper.mapper.schema.ColumnMetaData;
import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;
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

    @Override public List<BeanProperty> beanProperties (Connection conn, Class<?> beanType, TableMetaData tableMetaData) {
        return executeUnchecked(() -> {
            final AVector<Method> getters = wrap(beanType.getMethods())
                    .filterNot(m -> m.getName().equals("hashCode") || m.getName().equals("toString") || m.getName().equals("getClass"))
                    .filterNot(m -> m.getReturnType() == void.class)
                    .filter(m -> m.getParameterCount() == 0 && (m.getModifiers() & Modifier.STATIC) == 0)
                    .toVector();

            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();

            return getters.flatMap(getter -> executeUnchecked(() -> {
                final Method setter = beanType.getMethod("with" + toFirstUpper(getter.getName()), getter.getReturnType());
                final Method builderSetter = builderClass.getMethod(getter.getName(), getter.getReturnType());

                final Ignore getterIgnore = getter.getAnnotation(Ignore.class);

                if (getterIgnore != null && getterIgnore.value())
                    return AOption.empty();

                final Column columnAnnot = getter.getAnnotation(Column.class);
                final String columnName = AOption.of(columnAnnot).map(Column::value).orElse(getter.getName());
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

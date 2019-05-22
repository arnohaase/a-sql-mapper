package com.ajjpj.asqlmapper.javabeans.extractors;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.annotations.Ignore;
import com.ajjpj.asqlmapper.javabeans.columnnames.ColumnNameExtractor;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LombokWithBuilderMetaDataExtractor implements BeanMetaDataExtractor {
    private static final Logger log = LoggerFactory.getLogger(LombokWithBuilderMetaDataExtractor.class);

    private final ColumnNameExtractor columnNameExtractor;

    private final String builderFactoryName;
    private final String builderFinalizeMethodName;

    public LombokWithBuilderMetaDataExtractor(ColumnNameExtractor columnNameExtractor) {
        this(columnNameExtractor, "builder", "build");
    }

    public LombokWithBuilderMetaDataExtractor(ColumnNameExtractor columnNameExtractor, String builderFactoryName, String builderFinalizeMethodName) {
        this.columnNameExtractor = columnNameExtractor;
        this.builderFactoryName = builderFactoryName;
        this.builderFinalizeMethodName = builderFinalizeMethodName;
    }

    private boolean hasIgnoreAnnotation(Class<?> beanType, Method mtd) {
        return BeanReflectionHelper.allSuperMethods(beanType, mtd).exists(g ->
                g.getAnnotation(Ignore.class) != null && g.getAnnotation(Ignore.class).value()
        );
    }

    @Override
    public boolean canHandle(Class<?> cls) {
        try {
            builderFactoryFor(cls);
            builderFinalizerFor(cls);
            return true;
        }
        catch (Exception exc) {
            return false;
        }
    }

    @Override
    public AVector<BeanProperty> beanProperties(Class<?> beanType) {
        return executeUnchecked(() -> {
            final AVector.Builder<BeanProperty> result = AVector.builder();

            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();

            BeanExtractorUtils
                    .javaBeanGetters(beanType)
                    .forEach(getter -> {
                        getter.setAccessible(true);

                        final String propertyName = BeanExtractorUtils.javaBeanPropertyNameFor(getter);
                        final Class<?> propertyType = getter.getReturnType();

                        final Optional<Method> setter = BeanExtractorUtils
                                .wither(beanType, Optional.of("with"), propertyName, propertyType);

                        setter.ifPresent(s -> s.setAccessible(true));

                        final Method builderSetter = BeanExtractorUtils
                                .wither(builderClass, Optional.empty(), propertyName, getter.getReturnType())
                                .orElseThrow(() -> new IllegalArgumentException("no setter on builder " + builderClass + " for property " + getter.getName()));

                        final Optional<Field> field = BeanExtractorUtils.propField(beanType, propertyName);

                        final String columnName = columnNameExtractor.columnNameFor(beanType, getter, propertyName);

                        if (!BeanExtractorUtils.hasIgnoreAnnotation(beanType, getter, field)) {
                            result.add(new BeanProperty(beanType, propertyType, builderSetter.getParameterTypes()[0], propertyName, columnName, getter, setter, true,
                                    field, builderSetter, true));
                        }
                    });

            return result.build();
        });
    }

    @Override
    public Supplier<Object> builderFactoryFor(Class<?> beanType) {
        return executeUnchecked(() -> {
            final Method mtd = beanType.getMethod(builderFactoryName);
            mtd.setAccessible(true);
            return () -> executeUnchecked(() -> mtd.invoke(null));
        });
    }

    @Override
    public Function<Object, Object> builderFinalizerFor(Class<?> beanType) {
        return executeUnchecked(() -> {
            final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();
            final Method mtd = builderClass.getMethod(builderFinalizeMethodName);

            return builder -> executeUnchecked(() -> mtd.invoke(builder));
            //TODO InvocationTargetException
        });
    }
}

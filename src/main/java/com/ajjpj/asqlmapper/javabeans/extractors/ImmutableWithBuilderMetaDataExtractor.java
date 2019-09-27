package com.ajjpj.asqlmapper.javabeans.extractors;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.columnnames.ColumnNameExtractor;

public class ImmutableWithBuilderMetaDataExtractor implements BeanMetaDataExtractor {
    private final ColumnNameExtractor columnNameExtractor;

    private final String builderFactoryName;
    private final String builderFinalizeMethodName;
    private final String setterPrefix;

    public ImmutableWithBuilderMetaDataExtractor(ColumnNameExtractor columnNameExtractor) {
        this(columnNameExtractor, "builder", "build", "with");
    }

    public ImmutableWithBuilderMetaDataExtractor(ColumnNameExtractor columnNameExtractor, String builderFactoryName, String builderFinalizeMethodName,
                                                 String setterPrefix) {
        this.columnNameExtractor = columnNameExtractor;
        this.builderFactoryName = builderFactoryName;
        this.builderFinalizeMethodName = builderFinalizeMethodName;
        this.setterPrefix = setterPrefix;
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
        final Class<?> builderClass = builderFactoryFor(beanType).get().getClass();

        final AVector.Builder<BeanProperty> result = AVector.builder();

        BeanExtractorUtils
                .noPrefixGetters(beanType)
                .forEach(getter -> {
                    if (BeanExtractorUtils.hasIgnoreAnnotation(beanType, getter, Optional.empty())) {
                        return;
                    }

                    final Optional<Method> setter = BeanExtractorUtils
                            .wither(beanType, Optional.ofNullable(setterPrefix), getter.getName(), getter.getReturnType());
                    final Method builderSetter = BeanExtractorUtils
                            .wither(builderClass, Optional.empty(), getter.getName(), getter.getReturnType())
                            .orElseThrow(() -> new IllegalStateException("no setter on builder " + builderClass + " for property " + getter.getName()));

                    final String columnName = columnNameExtractor.columnNameFor(beanType, getter, getter.getName());

                    result.add(
                            new BeanProperty(beanType, builderSetter.getParameterTypes()[0], getter.getGenericReturnType(), getter.getName(), columnName, getter, setter,
                                    true, Optional.empty(), builderSetter, true));
                });

        return result.build();
    }

    @Override
    public Supplier<Object> builderFactoryFor(Class<?> beanType) {
        return BeanExtractorUtils.builderFactoryFor(beanType, builderFactoryName);
    }

    @Override
    public Function<Object, Object> builderFinalizerFor(Class<?> beanType) {
        return BeanExtractorUtils.builderFinalizerFor(beanType, builderFactoryName, builderFinalizeMethodName);
    }
}

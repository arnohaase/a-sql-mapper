package com.ajjpj.asqlmapper.javabeans.extractors;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.javabeans.BeanProperty;
import com.ajjpj.asqlmapper.javabeans.columnnames.ColumnNameExtractor;

public class JavaBeansMetaDataExtractor implements BeanMetaDataExtractor {
    private final ColumnNameExtractor columnNameExtractor;
    private final boolean requirePublicSetter;
    private final boolean requireVoidSetter;

    public JavaBeansMetaDataExtractor(ColumnNameExtractor columnNameExtractor) {
        this(columnNameExtractor, false, false);
    }

    public JavaBeansMetaDataExtractor(ColumnNameExtractor columnNameExtractor, boolean requirePublicSetter, boolean requireVoidSetter) {
        this.columnNameExtractor = columnNameExtractor;
        this.requirePublicSetter = requirePublicSetter;
        this.requireVoidSetter = requireVoidSetter;
    }

    @Override
    public boolean canHandle(Class<?> cls) {
        try {
            cls.getConstructor().newInstance();
            return true;
        }
        catch (Exception exc) {
            return false;
        }
    }

    @Override
    public AVector<BeanProperty> beanProperties(Class<?> beanType) {
        final AVector.Builder<BeanProperty> result = AVector.builder();

        BeanExtractorUtils
                .javaBeanGetters(beanType)
                .forEach(getter -> {
                    final String propertyName = BeanExtractorUtils.javaBeanPropertyNameFor(getter);
                    final Class<?> propertyType = getter.getReturnType();

                    final Optional<Method> optSetter = BeanExtractorUtils
                            .javaBeanSetter(beanType, propertyName, propertyType, getter.getDeclaringClass(), requirePublicSetter, requireVoidSetter);

                    if (!optSetter.isPresent()) {
                        return;
                    }

                    final Optional<Field> field = BeanExtractorUtils.propField(beanType, propertyName);
                    final String columnName = columnNameExtractor.columnNameFor(beanType, getter, propertyName);

                    if (!BeanExtractorUtils.hasIgnoreAnnotation(beanType, getter, field)) {
                        result.add(new BeanProperty(propertyType, getter.getGenericReturnType(), propertyName, columnName, getter, optSetter, false,
                                field, optSetter.get(), false));
                    }
                });

        return result.build();
    }

    @Override
    public Supplier<Object> builderFactoryFor(Class<?> beanType) {
        return () -> executeUnchecked(() -> beanType.getConstructor().newInstance());
    }

    @Override
    public Function<Object, Object> builderFinalizerFor(Class<?> beanType) {
        return x -> x;
    }
}

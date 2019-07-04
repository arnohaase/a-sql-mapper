package com.ajjpj.asqlmapper.javabeans.extractors;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;
import static com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper.unchecked;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.javabeans.annotations.Ignore;
import com.ajjpj.asqlmapper.mapper.util.BeanReflectionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of commonly used building blocks for sharing between different 'bean' conventions
 */
@SuppressWarnings("WeakerAccess")
public class BeanExtractorUtils {
    private static final Logger log = LoggerFactory.getLogger(BeanExtractorUtils.class);

    private BeanExtractorUtils() {
    }

    private static final Set<String> specialMethodNames = ASet.of("toString", "getClass", "hashCode", "wait", "notify", "notifyAll", "clone");

    public static Stream<Method> javaBeanGetters(Class<?> beanClass) {
        return Arrays.stream(beanClass.getMethods())
                .filter(m -> m.getName().startsWith("get") &&
                        !specialMethodNames.contains(m.getName()) &&
                        !m.isSynthetic() &&
                        m.getParameterCount() == 0 &&
                        !Modifier.isStatic(m.getModifiers()) &&
                        m.getName().length() > 3);
    }

    public static Stream<Method> noPrefixGetters(Class<?> beanClass) {
        return Arrays.stream(beanClass.getMethods())
                .filter(m -> !specialMethodNames.contains(m.getName()) &&
                        !Modifier.isStatic(m.getModifiers()) &&
                        !m.isSynthetic() &&
                        m.getParameterCount() == 0);
    }

    public static String javaBeanPropertyNameFor(Method getter) {
        final String raw = getter.getName().substring(3);
        if (raw.length() == 1) {
            return raw.toLowerCase();
        }
        // e.g. property name 'URL' for method getURL
        if (Character.isUpperCase(raw.charAt(1))) {
            return raw;
        }
        return Character.toLowerCase(raw.charAt(0)) + raw.substring(1);
    }

    public static Optional<Method> javaBeanSetter(Class<?> beanClass, String propertyName, Class<?> propertyType, Class<?> getterClass, boolean mustBePublic,
                                                  boolean mustBeVoid) {
        final String setterName = "set" + propertyName;

        try {
            try {
                final Method candidate = beanClass.getMethod(setterName, propertyType);
                if (Modifier.isStatic(candidate.getModifiers())) {
                    log.warn("method " + candidate + " is a candidate for a setter method, but it is static");
                    return Optional.empty();
                }

                return Optional.ofNullable(verifyVoidSetter(candidate, mustBeVoid));
            }
            catch (NoSuchMethodException exc) {
                try {
                    final Method setter = getterClass.getMethod(setterName, propertyType);
                    if (Modifier.isStatic(setter.getModifiers())) {
                        log.warn("method " + setter + " is a candidate for a setter method, but it is static");
                        return Optional.empty();
                    }
                    if (mustBePublic) {
                        log.warn("method " + setter + " is a candidate for a setter method, but setters are required to be public by configuration");
                        return Optional.empty();
                    }

                    setter.setAccessible(true);
                    return Optional.ofNullable(verifyVoidSetter(setter, mustBeVoid));
                }
                catch (NoSuchMethodException e) {
                    return Optional.empty();
                }
            }
            catch (SecurityException exc) {
                AUnchecker.throwUnchecked(exc);
            }
        }
        catch (Exception exc) {
            log.warn("exception looking up a setter", exc);
        }
        return Optional.empty();
    }

    /**
     * a "setter" method on an immutable bean, returning a modified copy with a given field having a new value
     */
    public static Optional<Method> wither(Class<?> beanClass, Optional<String> prefix, String propertyName, Class<?> propertyType) {
        final String mtdName = prefix
                .map(x -> x + toFirstUpper(propertyName))
                .orElse(propertyName);

        return unchecked(() -> {
            final List<Method> candidates = Arrays.stream(beanClass.getMethods())
                    .filter(m -> m.getName().equals(mtdName) && m.getParameterCount() == 1 && m.getParameterTypes()[0].isAssignableFrom(propertyType))
                    .collect(Collectors.toList());

            switch (candidates.size()) {
                case 0:
                    return Optional.empty();
                case 1:
                    final Method result = candidates.get(0);
                    if (!beanClass.isAssignableFrom(result.getReturnType())) {
                        log.warn("method " + result + " is a candidate for a 'wither' method, but its return type does not match bean type " + beanClass);
                        return Optional.empty();
                    }

                    if (Modifier.isStatic(result.getModifiers())) {
                        log.warn("method " + result + " is a candidate for a 'wither' method, but it is static");
                        return Optional.empty();
                    }

                    return Optional.of(result);
                default:
                    log.warn("more than one candidate 'wither' method " + propertyName + " on class " + beanClass.getName() + ": " + candidates);
                    return Optional.empty();
            }
        });
    }

    public static Optional<Field> propField(Class<?> beanType, String propertyName) {
        try {
            final Field result = beanType.getField(propertyName);
            result.setAccessible(true);
            return Optional.of(result);
        }
        catch (NoSuchFieldException exc) {
            return Optional.empty();
        }
        catch (Exception exc) {
            log.warn("error retrieving field " + propertyName + " in " + beanType, exc);
            return Optional.empty();
        }
    }

    public static Supplier<Object> builderFactoryFor(Class<?> beanType, String builderFactoryName) {
        return executeUnchecked(() -> {
            final Method mtd = beanType.getMethod(builderFactoryName); //TODO check 'static', check return type
            if (!Modifier.isStatic(mtd.getModifiers())) {
                throw new IllegalArgumentException("method " + mtd + " is a candidate for a builder method, but it is not static");
            }

            return () -> unchecked(() -> mtd.invoke(null));
        });
    }

    public static Function<Object, Object> builderFinalizerFor(Class<?> beanType, String builderFactoryName, String builderFinalizeMethodName) {
        return executeUnchecked(() -> {
            final Class<?> builderClass = builderFactoryFor(beanType, builderFactoryName).get().getClass();

            try {
                final Method mtd = builderClass.getMethod(builderFinalizeMethodName);

                if (!beanType.equals(mtd.getReturnType())) {
                    throw new IllegalArgumentException("builder finalizer method " + mtd + " does not return bean type " + beanType);
                }
                if (Modifier.isStatic(mtd.getModifiers())) {
                    throw new IllegalArgumentException("builder finalizer method " + mtd + " is static");
                }

                return builder -> unchecked(() -> mtd.invoke(builder));
            }
            catch (NoSuchMethodException exc) {
                throw new IllegalArgumentException(
                        "builder " + builderClass + " for bean type " + beanType + " has no finalizer method " + builderFinalizeMethodName, exc);
            }
        });
    }

    private static String toFirstUpper(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static Method verifyVoidSetter(Method mtd, boolean mustBeVoid) {
        if (mustBeVoid && mtd.getReturnType() != void.class) {
            log.warn("method " + mtd + " is a candidate for a setter method, but setters are required to be void by configuration");
            return null;
        }
        return mtd;
    }

    public static boolean hasIgnoreAnnotation(Class<?> beanType, Method getter, Optional<Field> field) {
        if (BeanReflectionHelper.allSuperMethods(beanType, getter).exists(g ->
                g.getAnnotation(Ignore.class) != null && g.getAnnotation(Ignore.class).value()
        )) {
            return true;
        }

        return field.isPresent() &&
                field.get().getAnnotation(Ignore.class) != null &&
                field.get().getAnnotation(Ignore.class).value();
    }
}

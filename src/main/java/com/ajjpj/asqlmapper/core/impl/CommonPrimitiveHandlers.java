package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AHashSet;
import com.ajjpj.acollections.mutable.AMutableArrayWrapper;
import com.ajjpj.asqlmapper.core.PrimitiveTypeHandler;

import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


public class CommonPrimitiveHandlers {

    public static final PrimitiveTypeHandler STRING_HANDLER = new StringHandler();
    public static final PrimitiveTypeHandler BOOLEAN_HANDLER = new PassThroughHandler(ASet.of(Boolean.class, boolean.class));
    public static final PrimitiveTypeHandler NUMERIC_HANDLER = new NumericHandler();
    public static final PrimitiveTypeHandler ENUM_AS_STRING_HANDLER = new EnumAsStringHandler();
    public static final PrimitiveTypeHandler LOCAL_DATE_HANDLER = new LocalDateHandler();
    public static final PrimitiveTypeHandler LOCAL_TIME_HANDLER = new LocalTimeHandler();
    public static final PrimitiveTypeHandler INSTANT_HANDLER = new InstantHandler();

    //TODO UUID

    public static final ASet<Class<?>> NUMERIC_TYPES = ASet.of(
            Byte.class, byte.class, Short.class, short.class, Integer.class, int.class, Long.class, long.class,
            Float.class, float.class, Double.class, double.class,
            BigInteger.class, BigDecimal.class);

    public static class PassThroughHandler implements PrimitiveTypeHandler {
        private final AHashSet<Class<?>> classes;

        public PassThroughHandler (ASet<Class<?>> classes) {
            this.classes = classes.toSet();
        }

        @Override public boolean canHandle (Class<?> cls) {
            return classes.contains(cls);
        }

        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            //noinspection unchecked
            return (T) o;
        }

        @Override public Object toSql (Object o) {
            return o;
        }
    }

    public static class StringHandler implements PrimitiveTypeHandler {
        @Override public boolean canHandle (Class<?> cls) {
            return cls == String.class;
        }

        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            if (o == null) return null;

            if (o instanceof Clob) {
                return executeUnchecked(() -> {
                    final StringBuilder sb = new StringBuilder();
                    final Reader r = ((Clob) o).getCharacterStream();
                    int ch;
                    while ((ch = r.read()) != -1) sb.append((char) ch);
                    //noinspection unchecked
                    return (T) sb.toString();
                });
            }

            //noinspection unchecked
            return (T) String.valueOf(o);
        }

        @Override public Object toSql (Object o) {
            return o;
        }
    }

    public static class EnumAsStringHandler implements PrimitiveTypeHandler {
        private final Map<Class<?>, AMap<String,Object>> enumConstants = new ConcurrentHashMap<>();

        @Override public boolean canHandle (Class<?> cls) {
            return cls.isEnum();
        }

        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            if (o == null) return null;

            final AMap<String,Object> constants = enumConstants.computeIfAbsent(targetType, cls -> AMutableArrayWrapper
                    .wrap(targetType.getEnumConstants())
                    .map(x -> new AbstractMap.SimpleImmutableEntry<String,Object>(String.valueOf(x), x))
                    .toMap()
            );

            //noinspection unchecked
            return (T) constants.getOptional(String.valueOf(o)).orElseThrow(() -> new IllegalArgumentException("'" + o + "' is not a valid enum constant for enum " + targetType.getName()));
        }

        @Override public Object toSql (Object o) {
            return String.valueOf(o);
        }
    }

    public static class NumericHandler implements PrimitiveTypeHandler {
        @Override public boolean canHandle (Class<?> cls) {
            return NUMERIC_TYPES.contains(cls);
        }

        @SuppressWarnings("unchecked")
        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            if (o == null) return null;
            if (o.getClass() == targetType) return (T) o;

            final Number n = (Number) o;
            if (targetType.isPrimitive()) {
                if (targetType == long.class) return (T)((Long) n.longValue());
                if (targetType == int.class) return (T)((Integer) n.intValue());
                if (targetType == short.class) return (T)((Short) n.shortValue());
                if (targetType == byte.class) return (T)((Byte) n.byteValue());
                if (targetType == double.class) return (T)((Double) n.doubleValue());
                if (targetType == float.class) return (T)((Float) n.floatValue());
            }
            else {
                if (targetType == Long.class) return (T)((Long) n.longValue());
                if (targetType == Integer.class) return (T)((Integer) n.intValue());
                if (targetType == Short.class) return (T)((Short) n.shortValue());
                if (targetType == Byte.class) return (T)((Byte) n.byteValue());
                if (targetType == Double.class) return (T)((Double) n.doubleValue());
                if (targetType == Float.class) return (T)((Float) n.floatValue());

                if (targetType == BigInteger.class) {
                    if (n instanceof BigDecimal) return (T) ((BigDecimal)n).toBigInteger();
                    return (T) BigInteger.valueOf(n.longValue());
                }
                if (targetType == BigDecimal.class) {
                    if (n instanceof BigInteger) return (T) new BigDecimal((BigInteger) n);
                    if (n instanceof Double || n instanceof Float) return (T) BigDecimal.valueOf(n.doubleValue());
                    return (T) BigDecimal.valueOf(n.longValue());
                }
            }

            throw new IllegalArgumentException(targetType.getName() + " is not a numeric type or can not be converted from " + o + " [" + o.getClass().getName() + "]");
        }

        @Override public Object toSql (Object o) {
            return o;
        }
    }

    public static class LocalTimeHandler implements PrimitiveTypeHandler {
        @Override public boolean canHandle (Class<?> cls) {
            return cls == LocalTime.class;
        }

        @SuppressWarnings("unchecked")
        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            if (o instanceof java.sql.Time) return (T) ((java.sql.Time) o).toLocalTime();
            throw new IllegalArgumentException("can not convert " + o + " of type " + o.getClass().getName() + " to type " + targetType);
        }

        @Override public Object toSql (Object o) {
            if (o instanceof LocalTime) return java.sql.Time.valueOf((LocalTime)o);
            return o;
        }
    }
    public static class LocalDateHandler implements PrimitiveTypeHandler {
        @Override public boolean canHandle (Class<?> cls) {
            return cls == LocalDate.class;
        }

        @SuppressWarnings("unchecked")
        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            if (o == null) return null;
            if (o.getClass() == targetType) return (T) o;
            if (o instanceof java.sql.Date) return (T) ((java.sql.Date) o).toLocalDate();
            throw new IllegalArgumentException("can not convert " + o + " of type " + o.getClass().getName() + " to type " + targetType);
        }

        @Override public Object toSql (Object o) {
            if (o instanceof LocalDate) return java.sql.Date.valueOf((LocalDate)o);
            return o;
        }
    }

    public static class InstantHandler implements PrimitiveTypeHandler {
        @Override public boolean canHandle (Class<?> cls) {
            return cls == Instant.class;
        }

        @Override public <T> T fromSql (Class<T> targetType, Object o) {
            //noinspection unchecked
            return (T) ((java.sql.Timestamp)o).toInstant();
        }

        @Override public Object toSql (Object o) {
            return java.sql.Timestamp.from((Instant)o);
        }
    }
}

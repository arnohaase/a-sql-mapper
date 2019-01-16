package com.ajjpj.asqlmapper.core;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.immutable.AHashMap;
import com.ajjpj.asqlmapper.core.impl.CanHandleRegistry;

import java.util.function.Function;

import static com.ajjpj.asqlmapper.core.impl.CommonPrimitiveHandlers.*;

/**
 * Convenience class for all aspects of dealing with primitive types
 */
public class PrimitiveTypeRegistry {
    public static PrimitiveTypeRegistry defaults() {
        final AHashMap<Class<?>, Function<Object,Object>> raw = AMap.of(
                java.sql.Date.class, x -> ((java.sql.Date)x).toLocalDate(),
                java.sql.Time.class, x -> ((java.sql.Time)x).toLocalTime(),
                java.sql.Timestamp.class, x -> ((java.sql.Timestamp)x).toInstant()
                //TODO CLOB?
        );

        final CanHandleRegistry<PrimitiveTypeHandler> handlers = CanHandleRegistry
                .create(STRING_HANDLER, BOOLEAN_HANDLER, NUMERIC_HANDLER, ENUM_AS_STRING_HANDLER, LOCAL_DATE_HANDLER, LOCAL_TIME_HANDLER, INSTANT_HANDLER);
        return new PrimitiveTypeRegistry(handlers, raw);
    }

    private final CanHandleRegistry<PrimitiveTypeHandler> handlers;
    private final AHashMap<Class<?>, Function<Object,Object>> rawTypeMappings;

    private PrimitiveTypeRegistry (CanHandleRegistry<PrimitiveTypeHandler> handlers, AHashMap<Class<?>, Function<Object, Object>> rawTypeMappings) {
        this.handlers = handlers;
        this.rawTypeMappings = rawTypeMappings;
    }

    public boolean isPrimitiveType(Class<?> cls) {
        return handlers.handlerFor(cls).isDefined();
    }

    public Object fromSql(Object o) {
        if (o == null) return null;
        return rawTypeMappings
                .getOptional(o.getClass())
                .map(x -> x.apply(o))
                .orElse(o);
    }

    public Object toSql(Object o) {
        if (o == null) return null;
        return handlers.handlerFor(o.getClass()).orElseThrow(() -> new IllegalArgumentException("no handler for " + o.getClass())).toSql(o);
    }
    public <T> T fromSql(Class<T> targetType, Object o) {
        if (o == null) return null;
        if (o.getClass() == targetType) //noinspection unchecked
            return (T) o;

        return handlers.handlerFor(targetType).orElseThrow(() -> new IllegalArgumentException("no handler for " + targetType)).fromSql(targetType, o);
    }

    public <T> PrimitiveTypeRegistry withRawTypeMapping(Class<T> jdbcType, Function<T, Object> rawMapping) {
        //noinspection unchecked
        return new PrimitiveTypeRegistry(handlers, rawTypeMappings.plus(jdbcType, (Function) rawMapping));
    }
    public PrimitiveTypeRegistry withHandler(PrimitiveTypeHandler handler) {
        return new PrimitiveTypeRegistry(handlers.withHandler(handler), rawTypeMappings);
    }
}

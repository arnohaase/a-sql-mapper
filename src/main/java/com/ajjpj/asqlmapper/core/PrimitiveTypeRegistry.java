package com.ajjpj.asqlmapper.core;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.immutable.AHashMap;
import com.ajjpj.asqlmapper.core.impl.CanHandleRegistry;

import java.util.function.Function;

import static com.ajjpj.asqlmapper.core.common.CommonPrimitiveHandlers.*;

/**
 * Handler class for dealing with primitive types, especially for converting them from and to database representations.
 *  Concrete behavior can be configured by registering {@link PrimitiveTypeHandler} instances or raw mappings for
 *  specific types.<p>
 *
 * Calling {@link #withHandler(PrimitiveTypeHandler)} registers a primitive type handler that is called when a primitive
 *  value is bound to a {@link java.sql.PreparedStatement} or read from a {@link java.sql.ResultSet} into a known type.
 *  When a value is read from a {@link java.sql.ResultSet} in a generic fashion - e.g. by calling
 *  {@link SqlEngine#rawQuery(String, Object...)} - then raw mappings are applied.<p>
 *
 * A {@code PrimitiveTypeRegistry} is immutable; calls to {@link #withHandler(PrimitiveTypeHandler)} or
 *  {@link #withRawTypeMapping(Class, Function)} return modified copies. This allows registering additional handlers
 *  for specific calls or contexts without affecting the rest of the application.<p>
 *
 * When a primitive type needs handling, registered handlers are checked last-to-first, and when a matching handler
 *  is found, it is used and no further checks performed. Last-to-first check order is used so that registering a new
 *  handler can override previously registered handlers.<p>
 *
 * A pre-configured instance with support for typical primitive types is available using {@link #defaults()}; this is
 *  what you want in the vast majority of cases. For the rare occasions when you want full control however you can
 *  start with {@link #empty()} and build up your registry from there.
 */
public class PrimitiveTypeRegistry {
    /**
      * @return a pre-configured registry instance with support for all common primitive types including
     *  {@code java.time.*} types and enums as strings. This is usually what you want to start with.
     */
    public static PrimitiveTypeRegistry defaults() {
        final AHashMap<Class<?>, Function<Object,Object>> raw = AMap.of(
                java.sql.Date.class, x -> ((java.sql.Date)x).toLocalDate(),
                java.sql.Time.class, x -> ((java.sql.Time)x).toLocalTime(),
                java.sql.Timestamp.class, x -> ((java.sql.Timestamp)x).toInstant()
        );

        final CanHandleRegistry<PrimitiveTypeHandler> handlers = CanHandleRegistry
                .create(STRING_HANDLER, BOOLEAN_HANDLER, NUMERIC_HANDLER, ENUM_AS_STRING_HANDLER, LOCAL_DATE_HANDLER, LOCAL_TIME_HANDLER, INSTANT_HANDLER,
                        BLOB_ETC_PASSTHROUGH_HANDLER);
        return new PrimitiveTypeRegistry(handlers, raw);
    }

    /**
     * @return a truly empty registry instance that knows of no primitive types. This is basically useless as it is,
     *  but in the rare situation when you want full control, you can start here and build up exactly what you need.
     */
    public static PrimitiveTypeRegistry empty() {
        return new PrimitiveTypeRegistry(CanHandleRegistry.empty(), AHashMap.empty());
    }

    private final CanHandleRegistry<PrimitiveTypeHandler> handlers;
    private final AHashMap<Class<?>, Function<Object,Object>> rawTypeMappings;

    private PrimitiveTypeRegistry (CanHandleRegistry<PrimitiveTypeHandler> handlers, AHashMap<Class<?>, Function<Object, Object>> rawTypeMappings) {
        this.handlers = handlers;
        this.rawTypeMappings = rawTypeMappings;
    }

    /**
     * Checks if a primitive type handler was registered that can handle a given application class.
     */
    public boolean isPrimitiveType(Class<?> cls) {
        return handlers.handlerFor(cls).isDefined();
    }

    /**
     * Converts a raw value from a {@link java.sql.ResultSet#getObject(String) ResultSet.getObject()} to its default
     *  representation (e.g. {@link java.sql.Timestamp} to {@link java.time.Instant}). This method is called when no
     *  specific application type is known. It uses raw mappings registered using
     *  {@link #withRawTypeMapping(Class, Function)}.
     */
    public Object fromSql(Object o) {
        if (o == null) return null;
        return rawTypeMappings
                .getOptional(o.getClass())
                .map(x -> x.apply(o))
                .orElse(o);
    }

    /**
     * Converts a value before setting it on a {@link java.sql.PreparedStatement PreparedStatement}. The object's type
     *  is used to look up a {@link PrimitiveTypeHandler} registered using {@link #withHandler(PrimitiveTypeHandler)}.
     *
     * @throws IllegalArgumentException if no matching handler was registered
     */
    public Object toSql(Object o) {
        if (o == null) return null;
        return handlers.handlerFor(o.getClass()).orElseThrow(() -> new IllegalArgumentException("no handler for " + o.getClass())).toSql(o);
    }

    /**
     * Converts a value from a {@link java.sql.ResultSet ResultSet} to a known application type. This type is used to
     *  look up a {@link PrimitiveTypeHandler} registered using {@link #withHandler(PrimitiveTypeHandler)}.
     *
     * @throws IllegalArgumentException if no matching handler was registered for this combination of types
     */
    public <T> T fromSql(Class<T> targetType, Object o) {
        if (o == null) return null;
        if (o.getClass() == targetType) //noinspection unchecked
            return (T) o;

        return handlers.handlerFor(targetType).orElseThrow(() -> new IllegalArgumentException("no handler for " + targetType)).fromSql(targetType, o);
    }

    /**
     * registers a new raw type mapping, returning a modified copy of the registry.
     * @return a modified copy of this registry, leaving the original unmodified.
     */
    public <T> PrimitiveTypeRegistry withRawTypeMapping(Class<T> jdbcType, Function<T, Object> rawMapping) {
        //noinspection unchecked
        return new PrimitiveTypeRegistry(handlers, rawTypeMappings.plus(jdbcType, (Function<Object,Object>) rawMapping));
    }

    /**
     * registers a new raw type handler, returning a modified copy of the registry.
     * @return a modified copy of this registry, leaving the original unmodified.
     */
    public PrimitiveTypeRegistry withHandler(PrimitiveTypeHandler handler) {
        return new PrimitiveTypeRegistry(handlers.withHandler(handler), rawTypeMappings);
    }
}

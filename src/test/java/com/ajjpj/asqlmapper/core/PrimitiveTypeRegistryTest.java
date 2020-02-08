package com.ajjpj.asqlmapper.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

import org.junit.jupiter.api.Test;

public class PrimitiveTypeRegistryTest {
    @Test void testIsPrimitive() {
        PrimitiveTypeRegistry registry = PrimitiveTypeRegistry.empty();

        assertFalse(registry.isPrimitiveType(String.class));

        registry = registry.withRawTypeMapping(String.class, x -> x);
        assertFalse(registry.isPrimitiveType(String.class));

        registry = registry.withHandler(new PrimitiveTypeHandler() {
            @Override public <T> T fromSql(Class<T> targetType, Object o) {
                throw new UnsupportedOperationException();
            }
            @Override public Object toSql(Object o) {
                throw new UnsupportedOperationException();
            }
            @Override public boolean canHandle(Class<?> cls) {
                return cls == String.class;
            }
        });
        assertTrue(registry.isPrimitiveType(String.class));
        assertFalse(registry.isPrimitiveType(int.class));
    }

    @Test void testFromSqlWithType() {
        PrimitiveTypeRegistry registry = PrimitiveTypeRegistry.empty();

        assertNull(registry.fromSql(String.class, null));

        PrimitiveTypeRegistry r2 = registry; // for use in lambda
        assertThrows(IllegalArgumentException.class, () -> r2.fromSql(Long.class, 1));

        PrimitiveTypeRegistry r3  = registry.withRawTypeMapping(Integer.class, Integer::longValue);
        assertThrows(IllegalArgumentException.class, () -> r3.fromSql(Long.class, 1));

        registry = registry.withHandler(new PrimitiveTypeHandler() {
            @Override public <T> T fromSql(Class<T> targetType, Object o) {
                //noinspection unchecked
                return (T) ((Long)((Integer) o).longValue());
            }
            @Override public Object toSql(Object o) {
                throw new UnsupportedOperationException();
            }
            @Override public boolean canHandle(Class<?> cls) {
                return cls == Long.class;
            }
        });
        assertEquals(Long.valueOf(1), registry.fromSql(Long.class, 1));
    }

    @Test void testToSql() {
        assertThrows(IllegalArgumentException.class, () -> PrimitiveTypeRegistry.empty().toSql(1L));

        assertNull(PrimitiveTypeRegistry.empty().toSql(null));

        final PrimitiveTypeRegistry r = PrimitiveTypeRegistry.empty().withHandler(new PrimitiveTypeHandler() {
            @Override public <T> T fromSql(Class<T> targetType, Object o) {
                throw new UnsupportedOperationException();
            }
            @Override public Object toSql(Object o) {
                return (Long)o + 1;
            }
            @Override public boolean canHandle(Class<?> cls) {
                return cls == Long.class;
            }
        });

        assertEquals(124L, r.toSql(123L));
        assertThrows(IllegalArgumentException.class, () -> r.toSql(123));
    }

    @Test void testFromSqlRaw() {
        assertEquals(1, PrimitiveTypeRegistry.empty().fromSql(1));
        assertNull(PrimitiveTypeRegistry.empty().fromSql(null));

        final PrimitiveTypeRegistry r = PrimitiveTypeRegistry.empty().withRawTypeMapping(Long.class, String::valueOf);
        assertEquals("123", r.fromSql(123L));
        assertEquals(123, r.fromSql(123));
    }

    @Test void testDefaults() {
        assertTrue(PrimitiveTypeRegistry.defaults().isPrimitiveType(int.class));
        assertTrue(PrimitiveTypeRegistry.defaults().isPrimitiveType(String.class));
        assertFalse(PrimitiveTypeRegistry.defaults().isPrimitiveType(StringBuilder.class));

        assertEquals(Instant.ofEpochMilli(987654321), PrimitiveTypeRegistry.defaults().fromSql(new java.sql.Timestamp(987654321)));
    }
}

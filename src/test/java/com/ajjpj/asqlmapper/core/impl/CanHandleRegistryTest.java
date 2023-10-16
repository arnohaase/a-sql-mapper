package com.ajjpj.asqlmapper.core.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CanHandleRegistryTest {
    private CanHandleRegistry<TestCanHandle> registry;

    @BeforeEach
    void setUp() {
        this.registry = CanHandleRegistry.empty();
    }

    @Test void testOverride() {
        assertTrue(registry.handlerFor(String.class).isEmpty());

        registry = registry.withHandler(new TestCanHandle("String", String.class));
        assertEquals("String", registry.handlerFor(String.class).get().id);

        registry = registry.withHandler(new TestCanHandle("String2", String.class));
        assertEquals("String2", registry.handlerFor(String.class).get().id);
    }
    
    @Test void testPartialOverride() {
        assertTrue(registry.handlerFor(Number.class).isEmpty());
        assertTrue(registry.handlerFor(Long.class).isEmpty());

        registry = registry.withHandler(new TestCanHandle("Number", Number.class));
        assertEquals("Number", registry.handlerFor(Number.class).get().id);
        assertEquals("Number", registry.handlerFor(Long.class).get().id);

        registry = registry.withHandler(new TestCanHandle("Long", Long.class));
        assertEquals("Number", registry.handlerFor(Number.class).get().id);
        assertEquals("Long", registry.handlerFor(Long.class).get().id);

        assertEquals("Number", registry.handlerFor(Integer.class).get().id);
    }

    @Test void testReversePartialOverride() {
        assertTrue(registry.handlerFor(Number.class).isEmpty());
        assertTrue(registry.handlerFor(Long.class).isEmpty());

        registry = registry.withHandler(new TestCanHandle("Long", Long.class));
        assertTrue(registry.handlerFor(Number.class).isEmpty());
        assertEquals("Long", registry.handlerFor(Long.class).get().id);

        registry = registry.withHandler(new TestCanHandle("Number", Number.class));
        assertEquals("Number", registry.handlerFor(Number.class).get().id);
        assertEquals("Number", registry.handlerFor(Long.class).get().id);
    }

    @Test void testImmutable() {
        final CanHandleRegistry<TestCanHandle> r = registry.withHandler(new TestCanHandle("String", String.class));
        final CanHandleRegistry<TestCanHandle> r2 = r.withHandler(new TestCanHandle("String2", String.class));

        assertEquals("String", r.handlerFor(String.class).get().id);
        assertEquals("String2", r2.handlerFor(String.class).get().id);
    }
    @Test void testCreate() {
        registry = CanHandleRegistry.create(
                new TestCanHandle("Number", Number.class),
                new TestCanHandle("Long", Long.class)
        );

        assertEquals("Number", registry.handlerFor(Number.class).get().id);
        assertEquals("Long", registry.handlerFor(Long.class).get().id);
    }


    static class TestCanHandle implements CanHandle {
        final String id;
        final Class<?> cls;

        public TestCanHandle(String id, Class<?> cls) {
            this.id = id;
            this.cls = cls;
        }

        @Override public boolean canHandle(Class<?> cls) {
            return this.cls.isAssignableFrom(cls);
        }
    }
}

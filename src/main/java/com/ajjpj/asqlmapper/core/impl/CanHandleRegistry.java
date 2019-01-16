package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.immutable.ALinkedList;
import com.ajjpj.acollections.mutable.AMutableArrayWrapper;
import com.ajjpj.acollections.util.AOption;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class CanHandleRegistry<T extends CanHandle> {
    private final ALinkedList<T> handlers;

    // cache both presence and absence of a handler for a given type
    private final Map<Class<?>, AOption<T>> handlerCache = new ConcurrentHashMap<>();

    public static <T extends CanHandle> CanHandleRegistry<T> empty() {
        return new CanHandleRegistry<>(ALinkedList.empty());
    }
    public static <T extends CanHandle> CanHandleRegistry<T> create(T... handlers) {
        return AMutableArrayWrapper.wrap(handlers).foldLeft(empty(), CanHandleRegistry::withHandler);
    }

    private CanHandleRegistry (ALinkedList<T> handlers) {
        this.handlers = handlers;
    }

    public AOption<T> handlerFor (Class<?> cls) {
        final AOption<T> cached = handlerCache.get(cls);
        if (cached != null) return cached;

        final AOption<T> result = handlers.find(x -> x.canHandle(cls));
        handlerCache.put(cls, result);
        return result;
    }

    public CanHandleRegistry<T> withHandler (T handler) {
        return new CanHandleRegistry<>(handlers.prepend(handler));
    }
}

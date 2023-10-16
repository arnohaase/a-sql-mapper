package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.immutable.ALinkedList;
import com.ajjpj.acollections.mutable.AMutableArrayWrapper;
import com.ajjpj.acollections.util.AOption;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A generic registry of handlers which are picked based on a type. Clients can register a number
 *  of handlers by calling {@link #withHandler(CanHandle)} (usually at initialization time) and
 *  can then ask for "the correct" handler for a given class by calling {@link #handlerFor(Class)}.<p>
 *
 * More specifically, a call to {@link #handlerFor(Class)} traverses the registered handlers in
 *  *reverse* order of registration, calling {@link CanHandle#canHandle(Class)} on each to check
 *  if it is applicable and returning the first handler to return {@code true}. Results - both hits
 *  and misses - are stored in a {@code Map} internally to avoid lookup overhead. Registered handlers
 *  are traversed in reverse order of registration so that handlers registered later can override
 *  previously registered handlers.<p>
 *
 * Having handlers implement {@link CanHandle#canHandle(Class)} rather than just using a type itself
 *  as a key allows more flexibility: Some handlers may handle a give type and its subtypes, others
 *  may handle just a type and *not* its subtypes, and still others may handle a number of types.<p>
 *
 * CanHandleRegistry is thread safe and immutable, i.e. it holds no observable mutable state. Calls to
 *  {@link #withHandler(CanHandle)} create a modified copy of the registry, leaving the original
 *  unmodified. This allows idiomatic use where a 'default' registry is initialized at start-up time,
 *  and a specific handler is added for a limited scope without affecting the global registry. It is
 *  also the reason it has no methods to remove handlers.
 *
 * @param <T> the type of handler managed by a registry instance
 */
public class CanHandleRegistry<T extends CanHandle> {
    private final ALinkedList<T> handlers;

    // cache both presence and absence of a handler for a given type
    private final Map<Class<?>, AOption<T>> handlerCache = new ConcurrentHashMap<>();

    public static <T extends CanHandle> CanHandleRegistry<T> empty() {
        return new CanHandleRegistry<>(ALinkedList.empty());
    }
    @SafeVarargs
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

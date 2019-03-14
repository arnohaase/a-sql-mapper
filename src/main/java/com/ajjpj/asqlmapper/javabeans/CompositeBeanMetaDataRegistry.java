package com.ajjpj.asqlmapper.javabeans;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Predicate;

import com.ajjpj.acollections.immutable.ALinkedList;
import com.ajjpj.acollections.util.AOption;

public class CompositeBeanMetaDataRegistry implements BeanMetaDataRegistry {
    private final ALinkedList<Map.Entry<BeanMetaDataRegistry, Predicate<Class<?>>>> registries;

    private CompositeBeanMetaDataRegistry (ALinkedList<Map.Entry<BeanMetaDataRegistry, Predicate<Class<?>>>> registries) {
        this.registries = registries;
    }

    public CompositeBeanMetaDataRegistry withRegistry(BeanMetaDataRegistry r, Predicate<Class<?>> p) {
        return new CompositeBeanMetaDataRegistry(registries.prepend(new AbstractMap.SimpleImmutableEntry<>(r, p)));
    }

    private AOption<BeanMetaDataRegistry> registryFor(Class<?> beanType) {
        return registries.find(e -> e.getValue().test(beanType)).map(Map.Entry::getKey);
    }

    @Override public boolean canHandle (Class<?> cls) {
        return registryFor(cls).isPresent();
    }

    @Override public BeanMetaData getBeanMetaData (Class<?> beanType) {
        return registryFor(beanType).get().getBeanMetaData(beanType);
    }
}

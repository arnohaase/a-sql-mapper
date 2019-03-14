package com.ajjpj.asqlmapper.javabeans;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ajjpj.asqlmapper.javabeans.extractors.BeanMetaDataExtractor;

public class BeanMetaDataRegistryImpl implements BeanMetaDataRegistry {
    private final BeanMetaDataExtractor extractor;
    private final Map<Class<?>, BeanMetaData> cache = new ConcurrentHashMap<>();

    public BeanMetaDataRegistryImpl (BeanMetaDataExtractor extractor) {
        this.extractor = extractor;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return extractor.canHandle(cls);
    }

    @Override public BeanMetaData getBeanMetaData (Class<?> beanType) {
        return cache.computeIfAbsent(beanType,
                bt -> new BeanMetaData(
                        bt,
                        extractor.beanProperties(bt),
                        extractor.builderFactoryFor(bt),
                        extractor.builderFinalizerFor(bt)));
    }
}

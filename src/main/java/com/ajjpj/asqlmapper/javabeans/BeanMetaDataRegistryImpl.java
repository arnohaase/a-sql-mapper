package com.ajjpj.asqlmapper.javabeans;

import com.ajjpj.asqlmapper.javabeans.extractors.BeanMetaDataExtractor;

public class BeanMetaDataRegistryImpl implements BeanMetaDataRegistry {
    private final BeanMetaDataExtractor extractor;

    public BeanMetaDataRegistryImpl (BeanMetaDataExtractor extractor) {
        this.extractor = extractor;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return extractor.canHandle(cls);
    }

    @Override public BeanMetaData getBeanMetaData (Class<?> beanType) {
        return new BeanMetaData(
                beanType,
                extractor.beanProperties(beanType),
                extractor.builderFactoryFor(beanType),
                extractor.builderFinalizerFor(beanType));
    }
}

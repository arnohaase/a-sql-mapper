package com.ajjpj.asqlmapper.mapper.provided;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;


class ProvidedPropertiesImpl implements ProvidedProperties {
    private final AMap<String, ProvidedValues> properties;

    ProvidedPropertiesImpl (AMap<String, ProvidedValues> properties) {
        this.properties = properties;
    }

    @Override public boolean hasValuesFor (String name) {
        final ProvidedValues forName = properties.get(name.toLowerCase());
        return forName != null && forName.pkType() != null;
    }

    @Override public Class<?> pkType (String name) {
        final ProvidedValues forName = properties.get(name.toLowerCase());
        return forName.pkType();
    }

    @Override public AOption<Object> get (String name, Object pk) {
        final ProvidedValues forName = properties.get(name.toLowerCase());
        return forName.get(pk);
    }

    @Override public ProvidedProperties with (String propertyName, ProvidedValues providedValues) {
        return new ProvidedPropertiesImpl(properties.plus(propertyName, providedValues));
    }

    @Override public boolean nonEmpty () {
        return properties.nonEmpty();
    }
}

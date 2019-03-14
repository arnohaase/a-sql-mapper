package com.ajjpj.asqlmapper.core.provided;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;


class ProvidedPropertiesImpl implements ProvidedProperties {
    private final AMap<String, ProvidedValues> properties;
    private final AMap<String, String> referencedProperties;

    ProvidedPropertiesImpl (AMap<String, ProvidedValues> properties, AMap<String, String> referencedProperties) {
        this.properties = properties;
        this.referencedProperties = referencedProperties;
    }

    @Override public boolean hasValuesFor (String name) {
        final ProvidedValues forName = properties.get(name);
        return forName != null && forName.pkType() != null;
    }

    @Override public Class<?> pkType (String name) {
        final ProvidedValues forName = properties.get(name);
        return forName.pkType();
    }

    @Override public AOption<Object> get (String name, Object pk) {
        final ProvidedValues forName = properties.get(name);
        return forName.get(pk);
    }

    @Override public String referencedPropertyNameFor (String name) {
        return referencedProperties.get(name);
    }

    @Override public ProvidedProperties with (String propertyName, String referencedProperty, ProvidedValues providedValues) {
        return new ProvidedPropertiesImpl(
                properties.plus(propertyName, providedValues),
                referencedProperties.plus(propertyName, referencedProperty));
    }

    @Override public boolean nonEmpty () {
        return properties.nonEmpty();
    }
}

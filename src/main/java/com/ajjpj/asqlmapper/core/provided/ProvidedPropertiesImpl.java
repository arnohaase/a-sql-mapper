package com.ajjpj.asqlmapper.core.provided;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;


class ProvidedPropertiesImpl implements ProvidedProperties {
    private final AMap<String, ProvidedValues> properties;
    private final AMap<String, String> referencedColumns;

    ProvidedPropertiesImpl (AMap<String, ProvidedValues> properties, AMap<String, String> referencedColumns) {
        this.properties = properties;
        this.referencedColumns = referencedColumns;
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

    @Override public String referencedColumnNameFor (String name) {
        return referencedColumns.get(name);
    }

    @Override public ProvidedProperties with (String propertyName, String referencedColumn, ProvidedValues providedValues) {
        return new ProvidedPropertiesImpl(
                properties.plus(propertyName, providedValues),
                referencedColumns.plus(propertyName, referencedColumn));
    }

    @Override public boolean nonEmpty () {
        return properties.nonEmpty();
    }
}

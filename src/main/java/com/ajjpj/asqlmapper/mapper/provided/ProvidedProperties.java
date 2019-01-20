package com.ajjpj.asqlmapper.mapper.provided;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AOption;


public interface ProvidedProperties {
    static ProvidedProperties empty() {
        return new ProvidedPropertiesImpl(AMap.empty());
    }

    static ProvidedProperties of (String propertyName, ProvidedValues propertyValues) {
        return empty().with(propertyName, propertyValues);
    }

    boolean hasValuesFor(String name); //TODO case insensitive; //            if(optProvided.isDefined() && optProvided.get().size() > 0) {
    Class<?> pkType(String name);

    AOption<Object> get(String name, Object pk);

    ProvidedProperties with (String propertyName, ProvidedValues providedValues);

    boolean nonEmpty ();
}

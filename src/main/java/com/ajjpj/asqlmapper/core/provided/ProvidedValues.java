package com.ajjpj.asqlmapper.core.provided;

import com.ajjpj.acollections.util.AOption;

import java.util.Map;


public class ProvidedValues {
    private final Class<?> pkType;
    private final Map<?,?> values;

    private ProvidedValues (Class<?> pkType, Map<?, ?> values) {
        this.pkType = pkType;
        this.values = values;
    }

    public static ProvidedValues of (Class<?> pkType, Map<?,?> values) {
        return new ProvidedValues(pkType, values);
    }

    public Class<?> pkType () {
        return pkType;
    }

    public AOption<Object> get(Object pk) {
        if (values.containsKey(pk)) {
            return AOption.some(values.get(pk));
        }
        else {
            return AOption.empty();
        }
    }
    
    public Map<?,?> data() {
        return values;
    }
}

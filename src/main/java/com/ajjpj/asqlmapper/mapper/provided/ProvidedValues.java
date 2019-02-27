package com.ajjpj.asqlmapper.mapper.provided;

import com.ajjpj.acollections.util.AOption;

import java.util.Map;


public class ProvidedValues {
    private final Map<?,?> values;

    private ProvidedValues (Map<?, ?> values) {
        this.values = values;
    }

    public static ProvidedValues of (Map<?,?> values) {
        return new ProvidedValues(values);
    }

    public Class<?> pkType () {
        if (values.isEmpty()) return null;
        return values.keySet().iterator().next().getClass();
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

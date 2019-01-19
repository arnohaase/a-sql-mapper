package com.ajjpj.asqlmapper.mapper;


import com.ajjpj.acollections.util.AOption;

public interface ProvidedValues {
    static ProvidedValues empty() {
        return null; //TODO
    }

    AOption<Object> get(String name, Object pk);
}

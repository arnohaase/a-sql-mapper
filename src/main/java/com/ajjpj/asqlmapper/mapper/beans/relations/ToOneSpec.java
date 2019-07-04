package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.util.Objects;

import com.ajjpj.asqlmapper.mapper.schema.ForeignKeySpec;

public class ToOneSpec {
    private final ForeignKeySpec foreignKeySpec;
    private final Class<?> referencedClass;
    private final Class<?> keyType;

    public ToOneSpec(ForeignKeySpec foreignKeySpec, Class<?> referencedClass,Class<?> keyType) {
        this.foreignKeySpec = foreignKeySpec;
        this.referencedClass = referencedClass;
        this.keyType = keyType;
    }

    public ForeignKeySpec foreignKeySpec () {
        return foreignKeySpec;
    }

    public Class<?> elementClass () {
        return referencedClass;
    }

    public Class<?> keyType () {
        return keyType;
    }

    @Override public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ToOneSpec that = (ToOneSpec) o;
        return Objects.equals(foreignKeySpec, that.foreignKeySpec) &&
                Objects.equals(referencedClass, that.referencedClass) &&
                Objects.equals(keyType, that.keyType);
    }

    @Override public int hashCode () {
        return Objects.hash(foreignKeySpec, referencedClass, keyType);
    }

    @Override public String toString () {
        return "OneToManySpec{" +
                "foreignKeySpec=" + foreignKeySpec +
                ", referencedClass=" + referencedClass +
                ", keyType=" + keyType +
                '}';
    }
}

package com.ajjpj.asqlmapper.mapper.beans.relations;

import java.util.Objects;

import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;
import com.ajjpj.asqlmapper.mapper.schema.ForeignKeySpec;

public class OneToManySpec {
    private final ForeignKeySpec foreignKeySpec;
    private final Class<?> elementClass;
    private final CollectionBuildStrategy collectionBuildStrategy;
    private final Class<?> keyType;

    public OneToManySpec (ForeignKeySpec foreignKeySpec, Class<?> elementClass, CollectionBuildStrategy collectionBuildStrategy, Class<?> keyType) {
        this.foreignKeySpec = foreignKeySpec;
        this.elementClass = elementClass;
        this.collectionBuildStrategy = collectionBuildStrategy;
        this.keyType = keyType;
    }

    public ForeignKeySpec foreignKeySpec () {
        return foreignKeySpec;
    }

    public Class<?> elementClass () {
        return elementClass;
    }

    public CollectionBuildStrategy collectionBuildStrategy () {
        return collectionBuildStrategy;
    }

    public Class<?> keyType () {
        return keyType;
    }

    @Override public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OneToManySpec that = (OneToManySpec) o;
        return Objects.equals(foreignKeySpec, that.foreignKeySpec) &&
                Objects.equals(elementClass, that.elementClass) &&
                Objects.equals(collectionBuildStrategy, that.collectionBuildStrategy) &&
                Objects.equals(keyType, that.keyType);
    }

    @Override public int hashCode () {
        return Objects.hash(foreignKeySpec, elementClass, collectionBuildStrategy, keyType);
    }

    @Override public String toString () {
        return "OneToManySpec{" +
                "foreignKeySpec=" + foreignKeySpec +
                ", elementClass=" + elementClass +
                ", collectionBuildStrategy=" + collectionBuildStrategy +
                ", keyType=" + keyType +
                '}';
    }
}

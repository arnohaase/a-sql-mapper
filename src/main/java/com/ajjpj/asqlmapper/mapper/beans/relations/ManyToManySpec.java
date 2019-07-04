package com.ajjpj.asqlmapper.mapper.beans.relations;


import java.util.Objects;

import com.ajjpj.asqlmapper.core.common.CollectionBuildStrategy;

public class ManyToManySpec {
    private final String manyManyTable;
    private final String fkToOwner;
    private final String fkToCollection;

    private final String ownerPk;

    private final String collTable;
    private final String collPk;
    private final Class<?> elementClass;
    private final CollectionBuildStrategy collectionBuildStrategy;
    private final Class<?> keyType;

    public ManyToManySpec (String manyManyTable, String fkToOwner, String fkToCollection, String ownerPk,
                           String collTable, String collPk, Class<?> elementClass, CollectionBuildStrategy collectionBuildStrategy, Class<?> keyType) {
        this.manyManyTable = manyManyTable;
        this.fkToOwner = fkToOwner;
        this.fkToCollection = fkToCollection;
        this.ownerPk = ownerPk;
        this.collTable = collTable;
        this.collPk = collPk;
        this.elementClass = elementClass;
        this.collectionBuildStrategy = collectionBuildStrategy;
        this.keyType = keyType;
    }

    public String manyManyTable () {
        return manyManyTable;
    }

    public String fkToOwner () {
        return fkToOwner;
    }

    public String fkToCollection () {
        return fkToCollection;
    }

    public String ownerPk () {
        return ownerPk;
    }

    public String collTable() {
        return collTable;
    }

    public String collPk() {
        return collPk;
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
        ManyToManySpec that = (ManyToManySpec) o;
        return Objects.equals(manyManyTable, that.manyManyTable) &&
                Objects.equals(fkToOwner, that.fkToOwner) &&
                Objects.equals(fkToCollection, that.fkToCollection) &&
                Objects.equals(ownerPk, that.ownerPk) &&
                Objects.equals(collTable, that.collTable) &&
                Objects.equals(collPk, that.collPk) &&
                Objects.equals(elementClass, that.elementClass) &&
                Objects.equals(collectionBuildStrategy, that.collectionBuildStrategy) &&
                Objects.equals(keyType, that.keyType);
    }

    @Override public int hashCode () {
        return Objects.hash(manyManyTable, fkToOwner, fkToCollection, ownerPk, collTable, collPk, elementClass, collectionBuildStrategy, keyType);
    }

    @Override public String toString () {
        return "ManyToManySpec{" +
                "manyManyTable='" + manyManyTable + '\'' +
                ", fkToOwner='" + fkToOwner + '\'' +
                ", fkToCollection='" + fkToCollection + '\'' +
                ", ownerPk='" + ownerPk + '\'' +
                ", collTable='" + collTable + '\'' +
                ", collPk='" + collPk + '\'' +
                ", elementClass=" + elementClass +
                ", collectionBuildStrategy=" + collectionBuildStrategy +
                ", keyType=" + keyType +
                '}';
    }
}

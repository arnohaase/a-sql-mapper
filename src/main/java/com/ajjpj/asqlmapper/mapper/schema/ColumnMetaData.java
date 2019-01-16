package com.ajjpj.asqlmapper.mapper.schema;

import java.sql.JDBCType;
import java.util.Objects;


public class ColumnMetaData {
    public final String colName;
    public final Class<?> colClass;
    public final JDBCType colType;
    public final String colTypeName;
    public final int size;
    public final int precision;
    public final int scale;

    public final boolean isPrimaryKey;
    public final boolean isAutoIncrement;
    public final boolean isNullable;

    public ColumnMetaData (String colName, Class<?> colClass, JDBCType colType, String colTypeName, int size, int precision, int scale, boolean isPrimaryKey, boolean isAutoIncrement, boolean isNullable) {
        this.colName = colName;
        this.colClass = colClass;
        this.colType = colType;
        this.colTypeName = colTypeName;
        this.size = size;
        this.precision = precision;
        this.scale = scale;
        this.isPrimaryKey = isPrimaryKey;
        this.isAutoIncrement = isAutoIncrement;
        this.isNullable = isNullable;
    }

    @Override public String toString () {
        return "ColumnMetaData{" +
                "colName='" + colName + '\'' +
                ", colClass=" + colClass +
                ", colType=" + colType +
                ", colTypeName='" + colTypeName + '\'' +
                ", size=" + size +
                ", precision=" + precision +
                ", scale=" + scale +
                ", isPrimaryKey=" + isPrimaryKey +
                ", isAutoIncrement=" + isAutoIncrement +
                ", isNullable=" + isNullable +
                '}';
    }

    @Override
    public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMetaData that = (ColumnMetaData) o;
        return size == that.size &&
                precision == that.precision &&
                scale == that.scale &&
                isPrimaryKey == that.isPrimaryKey &&
                isAutoIncrement == that.isAutoIncrement &&
                isNullable == that.isNullable &&
                Objects.equals(colName, that.colName) &&
                Objects.equals(colClass, that.colClass) &&
                colType == that.colType &&
                Objects.equals(colTypeName, that.colTypeName);
    }

    @Override
    public int hashCode () {
        return Objects.hash(colName, colClass, colType, colTypeName, size, precision, scale, isPrimaryKey, isAutoIncrement, isNullable);
    }
}

package com.ajjpj.asqlmapper.mapper.schema;

import com.ajjpj.acollections.util.AOption;

import java.sql.JDBCType;
import java.util.Objects;


public class ColumnMetaData {
    private final String colName;
    private final AOption<Class<?>> colClass;
    private final JDBCType colType;
    private final String colTypeName;
    private final int size;
    private final int precision;
    private final int scale;

    private final boolean isPrimaryKey;
    private final boolean isAutoIncrement;
    private final boolean isNullable;

    public ColumnMetaData (String colName, AOption<Class<?>> colClass, JDBCType colType, String colTypeName, int size, int precision, int scale, boolean isPrimaryKey, boolean isAutoIncrement, boolean isNullable) {
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

    public String colName() {
        return colName;
    }
    public AOption<Class<?>> colClass() {
        return colClass;
    }
    public JDBCType colType() {
        return colType;
    }
    public String colTypeName() {
        return colTypeName;
    }
    public int size() {
        return size;
    }
    public int precision () {
        return precision;
    }
    public int scale () {
        return scale;
    }
    public boolean isPrimaryKey () {
        return isPrimaryKey;
    }
    public boolean isAutoIncrement () {
        return isAutoIncrement;
    }
    public boolean isNullable () {
        return isNullable;
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

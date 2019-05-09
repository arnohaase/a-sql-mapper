package com.ajjpj.asqlmapper.mapper.schema;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;

import java.util.Objects;

public class TableMetaData {
    private final String tableName;
    private final AVector<ColumnMetaData> columns;
    private final AVector<ForeignKeySpec> foreignKeys;

    public TableMetaData (String tableName, AVector<ColumnMetaData> columns, AVector<ForeignKeySpec> foreignKeys) {
        this.tableName = tableName;
        this.columns = columns;
        this.foreignKeys = foreignKeys;
    }

    public String tableName () {
        return tableName;
    }

    public AVector<ForeignKeySpec> foreignKeys() {
        return foreignKeys;
    }

    public AVector<ColumnMetaData> columns () {
        return columns;
    }

    public AOption<ColumnMetaData> findColByName(String name) {
        return columns.find(c -> c.colName().equalsIgnoreCase(name));
    }

    public AVector<ColumnMetaData> pkColumns() {
        return columns.filter(ColumnMetaData::isPrimaryKey);
    }

    @Override public String toString () {
        return "TableMetaData{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }

    @Override
    public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetaData that = (TableMetaData) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode () {
        return Objects.hash(tableName, columns);
    }
}

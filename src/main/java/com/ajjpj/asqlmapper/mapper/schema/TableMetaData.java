package com.ajjpj.asqlmapper.mapper.schema;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;

import java.util.Objects;

public class TableMetaData {
    public final String tableName;
    public final AVector<ColumnMetaData> columns;

    public TableMetaData (String tableName, AVector<ColumnMetaData> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public AOption<ColumnMetaData> findColByName(String name) {
        return columns.find(c -> c.colName.equalsIgnoreCase(name));
    }

    public AVector<ColumnMetaData> pkColumns() {
        return columns.filter(c -> c.isPrimaryKey);
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

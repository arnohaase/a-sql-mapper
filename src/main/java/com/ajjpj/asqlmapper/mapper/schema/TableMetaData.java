package com.ajjpj.asqlmapper.mapper.schema;

import java.util.List;
import java.util.Objects;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;

public class TableMetaData {
    private final String tableName;
    private final AVector<ColumnMetaData> columns;
    private final AVector<ForeignKeySpec> foreignKeys;

    public TableMetaData(String tableName, AVector<ColumnMetaData> columns, AVector<ForeignKeySpec> foreignKeys) {
        this.tableName = tableName;
        this.columns = columns;
        this.foreignKeys = foreignKeys;
    }

    public String tableName() {
        return tableName;
    }

    public ForeignKeySpec uniqueFkTo(String targetTableName) {
        final List<ForeignKeySpec> all = foreignKeys.filter(fk -> fk.pkTableName().equalsIgnoreCase(targetTableName)); //TODO optimize this? Caching?
        switch (all.size()) {
            case 0: throw new IllegalArgumentException("no foreign key from " + tableName + " to " + targetTableName);
            case 1: return all.get(0);
            default: throw new IllegalArgumentException("more than one foreign key from " + tableName + " to " + targetTableName + ": " + all);
        }
    }

    public AVector<ForeignKeySpec> foreignKeys() {
        return foreignKeys;
    }

    public AVector<ColumnMetaData> columns() {
        return columns;
    }

    public AOption<ColumnMetaData> findColByName(String name) {
        return columns.find(c -> c.colName().equalsIgnoreCase(name));
    }

    public AVector<ColumnMetaData> pkColumns() {
        //TODO optimize this? Caching?
        return columns.filter(ColumnMetaData::isPrimaryKey);
    }

    public ColumnMetaData getUniquePkColumn() {
        switch (pkColumns().size()) {
            case 0: throw new IllegalArgumentException("table " + tableName + " has no primary key");
            case 1: return pkColumns().get(0);
            default: throw new IllegalArgumentException("table " + tableName + " has more than one primary key: " + pkColumns());
        }
    }

    @Override
    public String toString() {
        return "TableMetaData{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableMetaData that = (TableMetaData) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns);
    }
}

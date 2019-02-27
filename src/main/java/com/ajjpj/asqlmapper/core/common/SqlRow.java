package com.ajjpj.asqlmapper.core.common;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;

import java.sql.ResultSet;


public class SqlRow {
    private final AMap<String,Object> byLowerCaseColumn;
    private final AVector<String> columnNames;
    private final PrimitiveTypeRegistry primTypes;

    public SqlRow (ResultSet rs, AVector<String> columnNames, PrimitiveTypeRegistry primTypes) {
        this.byLowerCaseColumn = columnNames.fold(AMap.empty(),
                (res, el) -> res.plus(el.toLowerCase(), AUnchecker.executeUnchecked(() -> rs.getObject(el))));
        this.columnNames = columnNames;
        this.primTypes = primTypes;
    }

    public int numColumns() {
        return columnNames.size();
    }

    public AVector<String> columnNames() {
        return columnNames;
    }

    public <T> T get(Class<T> cls, String columnName) {
        return primTypes.fromSql(cls, byLowerCaseColumn.get(columnName.toLowerCase()));
    }
    public Object get(String columnName) {
        return primTypes.fromSql(byLowerCaseColumn.get(columnName.toLowerCase()));
    }

    public <T> T get(Class<T> cls, int idx) {
        return get(cls, columnNames.get(idx));
    }
    public Object get(int idx) {
        return get(columnNames.get(idx));
    }

    @Override public String toString () {
        return "SqlRow{" + byLowerCaseColumn.mkString(",") + "}";
    }
}

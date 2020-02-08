package com.ajjpj.asqlmapper.core.common;

import java.sql.ResultSet;
import java.util.List;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;

/**
 * A SqlRow implementation that copies row data, making it available independently of the
 *  ResultSet it originated from. This class is immutable and 'safe' to use in all contexts,
 *  incurring some additional cost for copying and storing field values.
 */
public class DetachedSqlRow implements SqlRow {
    private final AMap<String,Object> byLowerCaseColumn;
    private final List<String> columnNames;
    private final PrimitiveTypeRegistry primTypes;

    public DetachedSqlRow (ResultSet rs, AList<String> columnNames, PrimitiveTypeRegistry primTypes) {
        this.byLowerCaseColumn = columnNames.fold(AMap.empty(),
                (res, el) -> res.plus(el.toLowerCase(), AUnchecker.executeUnchecked(() -> rs.getObject(el))));
        this.columnNames = columnNames;
        this.primTypes = primTypes;
    }

    @Override public DetachedSqlRow detach () {
        return this;
    }

    @Override public List<String> columnNames() {
        return columnNames;
    }

    @Override public <T> T get(Class<T> cls, String columnName) {
        return primTypes.fromSql(cls, byLowerCaseColumn.get(columnName.toLowerCase()));
    }
    @Override public Object get(String columnName) {
        return primTypes.fromSql(byLowerCaseColumn.get(columnName.toLowerCase()));
    }

    @Override public String toString () {
        final StringBuilder result = new StringBuilder(getClass().getSimpleName() + "{");
        boolean first = true;
        for(String colName: columnNames()) {
            if(first)
                first = false;
            else
                result.append(",");

            result.append(colName).append("->").append(get(colName));
        }

        result.append("}");
        return result.toString();
    }

    @Override public boolean equals(Object obj) {
        if(! (obj instanceof SqlRow)) {
            return false;
        }

        return byLowerCaseColumn.equals(((SqlRow)obj).detach().byLowerCaseColumn);
    }
}

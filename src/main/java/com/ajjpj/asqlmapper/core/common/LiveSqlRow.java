package com.ajjpj.asqlmapper.core.common;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;

/**
 * A SqlRow implementation that is a view to the underlying ResultSet. When the ResultSet
 *  moves on, so does this view's data, and if the ResultSet is closed, no data is available
 *  through this view any more. This class does not copy any data from the underlying
 *  ResultSet, avoiding the CPU and memory / garbage overhead associated with copying.<p>
 *
 * This implementation is useful for working with streams of data where processing of one row
 *  is often completed before moving on to the next, and the number of rows being processed
 *  can be huge, making reduced garbage production attractive.
 */
public class LiveSqlRow implements SqlRow {
    private final PrimitiveTypeRegistry primTypes;
    private final ResultSet rs;
    private final List<String> columnNames;

    public LiveSqlRow (PrimitiveTypeRegistry primTypes, ResultSet rs) throws SQLException {
        this.primTypes = primTypes;
        this.rs = rs;
        this.columnNames = new LiveColumnNames(rs);
    }

    @Override public DetachedSqlRow detach () {
        return new DetachedSqlRow(rs, AList.from(columnNames()), primTypes);
    }

    @Override public List<String> columnNames () {
        return columnNames;
    }

    @Override public <T> T get (Class<T> cls, String columnName) {
        return executeUnchecked(() -> primTypes.fromSql(cls, rs.getObject(columnName)));
    }

    @Override public Object get (String columnName) {
        return executeUnchecked(() -> primTypes.fromSql(rs.getObject(columnName)));
    }

    @Override public <T> T get (Class<T> cls, int idx) {
        return executeUnchecked(() -> primTypes.fromSql(cls, rs.getObject(idx+1)));
    }

    @Override public Object get (int idx) {
        return executeUnchecked(() -> primTypes.fromSql(rs.getObject(idx)));
    }

    static class LiveColumnNames extends AbstractList<String> {
        private final ResultSetMetaData rsMeta;

        LiveColumnNames (ResultSet rs) throws SQLException {
            this.rsMeta = rs.getMetaData();
        }

        @Override public String get (int index) {
            return executeUnchecked(() -> rsMeta.getColumnName(index+1));
        }

        @Override public int size () {
            return executeUnchecked(rsMeta::getColumnCount);
        }
    }
}

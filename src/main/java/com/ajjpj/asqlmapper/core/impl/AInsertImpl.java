package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.AInsert;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AInsertImpl<T> implements AInsert<T> {
    private final Class<T> pkCls;
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final RowExtractor rowExtractor;
    private final AVector<String> columnNames;
    private final AVector<SqlEngineEventListener> listeners;

    public AInsertImpl (Class<T> pkCls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                        AVector<String> columnNames, AVector<SqlEngineEventListener> listeners) {
        this.pkCls = pkCls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
        this.columnNames = columnNames;
        this.listeners = listeners;
    }

    @Override public T executeSingle (Connection conn) throws SQLException {
        listeners.forEach(l -> l.onBeforeInsert(sql, pkCls, columnNames));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql(), columnNames.toArray(new String[0]));
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                ps.executeUpdate();
                final ResultSet rs = ps.getGeneratedKeys();
                if (!rs.next()) throw new IllegalStateException("no result");
                final T result = rowExtractor.fromSql(pkCls, primTypes, rs, rowExtractor.mementoPerQuery(pkCls, primTypes, rs));
                if (rs.next()) throw new IllegalStateException("more than one result row");

                listeners.reverseIterator().forEachRemaining(l -> l.onAfterInsert(result));

                return result;
            }
            finally {
                SqlHelper.closeQuietly(ps);
            }
        }
        catch(Throwable th) {
            listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
            throw th;
        }
    }

    @Override public AList<T> executeMulti (Connection conn) throws SQLException {
        final PreparedStatement ps = conn.prepareStatement(sql.getSql(), columnNames.toArray(new String[0]));
        try {
            SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
            ps.executeUpdate();
            final AVector.Builder<T> result = AVector.builder();
            final ResultSet rs = ps.getGeneratedKeys();
            final Object memento = rowExtractor.mementoPerQuery(pkCls, primTypes, rs);
            while(rs.next()) result.add(rowExtractor.fromSql(pkCls, primTypes, rs, memento));
            return result.build();
        }
        finally {
            SqlHelper.closeQuietly(ps);
        }
    }
}

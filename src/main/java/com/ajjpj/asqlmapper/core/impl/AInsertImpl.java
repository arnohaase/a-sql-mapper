package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.AInsert;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.common.LiveSqlRow;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class AInsertImpl<T> implements AInsert<T> {
    private final Class<T> pkCls;
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final RowExtractor rowExtractor;
    private final AVector<String> columnNames;
    private final AVector<SqlEngineEventListener> listeners;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;

    public AInsertImpl (Class<T> pkCls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                        List<String> columnNames, AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier) {
        this.pkCls = pkCls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
        this.columnNames = AVector.from(columnNames);
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
    }

    @Override public T executeSingle () {
        return executeSingle(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());

    }
    @Override public T executeSingle (Connection conn) {
        listeners.forEach(l -> l.onBeforeInsert(sql, pkCls, columnNames));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql(), columnNames.toArray(new String[0]));
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                ps.executeUpdate();
                final ResultSet rs = ps.getGeneratedKeys();
                if (!rs.next()) throw new IllegalStateException("no result");
                final SqlRow row = new LiveSqlRow(primTypes, rs);
                final T result = rowExtractor.fromSql(pkCls, primTypes, row, rowExtractor.mementoPerQuery(pkCls, primTypes, rs, false), false, Collections.emptyMap());
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
            AUnchecker.throwUnchecked(th);
            return null; // for the compiler
        }
    }

    @Override public AList<T> executeMulti () {
        return executeMulti(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public AList<T> executeMulti (Connection conn) {
        listeners.forEach(l -> l.onBeforeInsert(sql, pkCls, columnNames));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql(), columnNames.toArray(new String[0]));
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                ps.executeUpdate();
                final AVector.Builder<T> builder = AVector.builder();
                final ResultSet rs = ps.getGeneratedKeys();
                final Object memento = rowExtractor.mementoPerQuery(pkCls, primTypes, rs, false);
                final SqlRow row = new LiveSqlRow(primTypes, rs);
                while (rs.next()) builder.add(rowExtractor.fromSql(pkCls, primTypes, row, memento, false, Collections.emptyMap()));
                final AList<T> result = builder.build();
                listeners.reverseIterator().forEachRemaining(l -> l.onAfterInsert(result));
                return result;
            }
            finally {
                SqlHelper.closeQuietly(ps);
            }
        }
        catch(Throwable th) {
            listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
            AUnchecker.throwUnchecked(th);
            return null; // for the compiler
        }
    }
}

package com.ajjpj.asqlmapper.core.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.mutable.AMutableListWrapper;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

public class ABatchUpdate {
    private final String sql;
    private final List<List<?>> params;
    private final PrimitiveTypeRegistry primTypes;
    private final AVector<SqlEngineEventListener> listeners;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;

    public ABatchUpdate(String sql, List<List<?>> params, PrimitiveTypeRegistry primTypes, AVector<SqlEngineEventListener> listeners,
                        AOption<Supplier<Connection>> defaultConnectionSupplier) {
        this.sql = sql;
        this.params = params;
        this.primTypes = primTypes;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
    }
    public ABatchUpdate(List<SqlSnippet> items, PrimitiveTypeRegistry primTypes, AVector<SqlEngineEventListener> listeners,
                        AOption<Supplier<Connection>> defaultConnectionSupplier) {
        this(sql(items), params(items), primTypes, listeners, defaultConnectionSupplier);
    }

    private static String sql(List<SqlSnippet> items) {
        if (items.isEmpty()) {
            throw new IllegalArgumentException("no batch items");
        }

        for (int i = 1; i < items.size(); i++) {
            if (!items.get(0).getSql().equals(items.get(i).getSql())) {
                throw new IllegalArgumentException(
                        "all batch items must have the same SQL - item " + i + " differed from item 0 (" + items.get(0).getSql() + " / " +
                                items.get(i).getSql());
            }
        }
        return items.get(0).getSql();
    }

    private static List<List<?>> params(List<SqlSnippet> items) {
        return AMutableListWrapper.wrap(items).map(SqlSnippet::getParams);
    }

    public int[] execute() {
        return execute(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    public int[] execute(Connection conn) {
        return doExecute(conn, PreparedStatement::executeBatch);
    }

    private <T> T doExecute(Connection conn, PsExecutor<T> executor) {
        listeners.forEach(l -> l.onBeforeBatchUpdate(sql, params.size()));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql);
            try {
                for (List<?> batchItem : params) {
                    SqlHelper.bindParameters(ps, batchItem, primTypes);
                    ps.addBatch();
                }

                final T result = executor.execute(ps);

                listeners.reverseIterator().forEachRemaining(SqlEngineEventListener::onAfterBatchUpdate);
                return result;
            }
            finally {
                SqlHelper.closeQuietly(ps);
            }
        }
        catch (Throwable th) {
            listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
            AUnchecker.throwUnchecked(th);
            return null;  // for the compiler
        }
    }

    public long[] executeLarge(Connection conn) {
        //noinspection ConstantConditions
        return doExecute(conn, PreparedStatement::executeLargeBatch);
    }
    public long[] executeLarge() {
        return executeLarge(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    private interface PsExecutor<T> {
        T execute(PreparedStatement ps) throws SQLException;
    }
}

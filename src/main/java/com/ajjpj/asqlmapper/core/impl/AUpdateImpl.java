package com.ajjpj.asqlmapper.core.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Supplier;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.AUpdate;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

public class AUpdateImpl implements AUpdate {
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final AVector<SqlEngineEventListener> listeners;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;

    public AUpdateImpl (SqlSnippet sql, PrimitiveTypeRegistry primTypes, AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier) {
        this.sql = sql;
        this.primTypes = primTypes;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
    }

    @Override public int execute () {
        return execute(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public int execute (Connection conn) {
        //noinspection ConstantConditions
        return doExecute(conn, PreparedStatement::executeUpdate);
    }

    private <T> T doExecute (Connection conn, PsExecutor<T> executor) {
        listeners.forEach(l -> l.onBeforeUpdate(sql));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql());
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                final T result = executor.execute(ps);

                listeners.reverseIterator().forEachRemaining(l -> l.onAfterUpdate(((Number)result).longValue()));
                return result;
            }
            finally {
                SqlHelper.closeQuietly(ps);
            }
        }
        catch(Throwable th) {
            listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
            AUnchecker.throwUnchecked(th);
            return null;  // for the compiler
        }
    }

    @Override public long executeLarge(Connection conn) {
        //noinspection ConstantConditions
        return doExecute(conn, PreparedStatement::executeLargeUpdate);
    }
    @Override public long executeLarge() {
        return executeLarge(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    private interface PsExecutor<T> {
        T execute(PreparedStatement ps) throws SQLException;
    }
}

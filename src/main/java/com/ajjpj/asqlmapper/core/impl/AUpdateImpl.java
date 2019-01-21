package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.AUpdate;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AUpdateImpl implements AUpdate {
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final AVector<SqlEngineEventListener> listeners;

    public AUpdateImpl (SqlSnippet sql, PrimitiveTypeRegistry primTypes, AVector<SqlEngineEventListener> listeners) {
        this.sql = sql;
        this.primTypes = primTypes;
        this.listeners = listeners;
    }

    @Override public int execute (Connection conn) throws SQLException {
        listeners.forEach(l -> l.onBeforeUpdate(sql));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql());
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                final int result = ps.executeUpdate();

                listeners.reverseIterator().forEachRemaining(l -> l.onAfterUpdate(result));
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
}

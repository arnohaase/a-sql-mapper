package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;


public class AQueryImpl<T> implements AQuery<T> {
    protected final Class<T> rowClass;
    protected final SqlSnippet sql;
    protected final PrimitiveTypeRegistry primTypes;
    private final RowExtractor rowExtractor;
    protected final AVector<SqlEngineEventListener> listeners;

    public AQueryImpl (Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                       AVector<SqlEngineEventListener> listeners) {
        this.rowClass = cls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
        this.listeners = listeners;
    }

    @Override public T single (Connection conn) throws SQLException {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) throw new IllegalStateException("no result");
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            final T result = doExtract(rs, memento);
            if (rs.next()) throw new IllegalStateException("more than one result row");
            afterIteration(1);
            return result;
        }));
    }

    private void afterIteration(int numRows) {
        listeners.reverseIterator().forEachRemaining(l -> l.onAfterQueryIteration(numRows));
    }

    private <X> X doQuery(Connection conn, Function<ResultSet, X> resultHandler) throws SQLException {
        listeners.forEach(l -> l.onBeforeQuery(sql, rowClass));
        try {
            final PreparedStatement ps = conn.prepareStatement(sql.getSql());
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                final ResultSet rs = ps.executeQuery();
                listeners.reverseIterator().forEachRemaining(SqlEngineEventListener::onAfterQueryExecution);
                return resultHandler.apply(rs);
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

    @Override public AOption<T> optional (Connection conn) throws SQLException {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) {
                afterIteration(0);
                return AOption.empty();
            }
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            final T result = doExtract(rs, memento);
            if (rs.next()) throw new IllegalStateException("more than one result row");
            afterIteration(1);
            return AOption.some(result);
        }));
    }

    @Override public AList<T> list (Connection conn) throws SQLException {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            final AVector.Builder<T> builder = AVector.builder();
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            while (rs.next()) builder.add(doExtract(rs, memento));
            final AList<T> result = builder.build();
            afterIteration(result.size());
            return result;
        }));
    }

    protected T doExtract(ResultSet rs, Object memento) throws SQLException {
        return rowExtractor.fromSql(rowClass, primTypes, rs, memento);
    }

    @Override public Stream<T> stream (Connection conn) {
        return StreamSupport.stream(() -> new ResultSetSpliterator(conn), Spliterator.ORDERED, false);
    }

    private class ResultSetSpliterator implements Spliterator<T> {
        private final PreparedStatement ps;
        private final ResultSet rs;
        private final Object memento;
        private int numRows = 0;

        ResultSetSpliterator(Connection conn) {
            try {
                listeners.forEach(l -> l.onBeforeQuery(sql, rowClass));
                ps = conn.prepareStatement(sql.getSql());
            }
            catch (Throwable th) {
                listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
                AUnchecker.throwUnchecked(th);
                throw new Error(); // for the compiler
            }
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                rs = ps.executeQuery();
                listeners.reverseIterator().forEachRemaining(SqlEngineEventListener::onAfterQueryExecution);
                memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            }
            catch (Throwable th) {
                SqlHelper.closeQuietly(ps);
                listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
                AUnchecker.throwUnchecked(th);
                throw new Error(); // for the compiler
            }
        }

        @Override public boolean tryAdvance (Consumer<? super T> action) {
            try {
                if (!rs.next()) {
                    SqlHelper.closeQuietly(ps);
                    afterIteration(numRows);
                    return false;
                }
                numRows += 1;
                action.accept(doExtract(rs, memento));
                return true;
            }
            catch(Throwable th) {
                listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
                SqlHelper.closeQuietly(ps);
                AUnchecker.throwUnchecked(th);
                return false; // dead code - for the compiler
            }
        }

        @Override public Spliterator<T> trySplit () {
            return null;
        }
        @Override public long estimateSize () {
            return Long.MAX_VALUE;
        }
        @Override public int characteristics () {
            return ORDERED;
        }
    }
}

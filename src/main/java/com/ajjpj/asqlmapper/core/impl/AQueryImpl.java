package com.ajjpj.asqlmapper.core.impl;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;
import com.ajjpj.asqlmapper.core.provided.ProvidedProperties;
import com.ajjpj.asqlmapper.core.provided.ProvidedValues;


public class AQueryImpl<T> implements AQuery<T> {
    protected final Class<T> rowClass;
    protected final SqlSnippet sql;
    protected final PrimitiveTypeRegistry primTypes;
    private final RowExtractor rowExtractor;
    protected final AVector<SqlEngineEventListener> listeners;
    protected final AOption<Supplier<Connection>> defaultConnectionSupplier;
    protected final ProvidedProperties providedValues;

    public AQueryImpl (Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                       AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier, ProvidedProperties providedValues) {
        this.rowClass = cls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
        this.providedValues = providedValues;
    }

    @Override public AQuery<T> withPropertyValues (String propName, String referencedColumnName, ProvidedValues providedValues) {
        return new AQueryImpl<>(rowClass, sql, primTypes, rowExtractor, listeners, defaultConnectionSupplier, this.providedValues.with(propName, referencedColumnName, providedValues));
    }

    @Override public T single () {
        return single(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public T single (Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) throw new IllegalStateException("no result");
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            final T result = doExtract(conn, rs, memento);
            if (rs.next()) throw new IllegalStateException("more than one result row");
            afterIteration(1);
            return result;
        }));
    }

    private void afterIteration(int numRows) {
        listeners.reverseIterator().forEachRemaining(l -> l.onAfterQueryIteration(numRows));
    }

    private <X> X doQuery(Connection conn, Function<ResultSet, X> resultHandler) {
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
            AUnchecker.throwUnchecked(th);
            throw new RuntimeException("just for the compiler");
        }
    }

    @Override public AOption<T> optional () {
        return optional(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public AOption<T> optional (Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) {
                afterIteration(0);
                return AOption.empty();
            }
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            final T result = doExtract(conn, rs, memento);
            if (rs.next()) throw new IllegalStateException("more than one result row");
            afterIteration(1);
            return AOption.some(result);
        }));
    }

    @Override public AList<T> list () {
        return list(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public AList<T> list (Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            final AVector.Builder<T> builder = AVector.builder();
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs);
            while (rs.next()) builder.add(doExtract(conn, rs, memento));
            final AList<T> result = builder.build();
            afterIteration(result.size());
            return result;
        }));
    }

    protected T doExtract(Connection conn, ResultSet rs, Object memento) throws SQLException {
        return rowExtractor.fromSql(rowClass, primTypes, rs, memento, providedValues);
    }

    @Override public Stream<T> stream () {
        return stream(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public Stream<T> stream (Connection conn) {
        return StreamSupport.stream(() -> new ResultSetSpliterator(conn), Spliterator.ORDERED, false);
    }

    private class ResultSetSpliterator implements Spliterator<T> {
        private final Connection conn;
        private final PreparedStatement ps;
        private final ResultSet rs;
        private final Object memento;
        private int numRows = 0;

        ResultSetSpliterator(Connection conn) {
            this.conn = conn;
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
                action.accept(doExtract(conn, rs, memento));
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

package com.ajjpj.asqlmapper.core.impl;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
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
import com.ajjpj.asqlmapper.core.common.LiveSqlRow;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

public class AQueryImpl<T> implements AQuery<T> {
    private final Class<T> rowClass;
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final RowExtractor rowExtractor;
    private final AVector<SqlEngineEventListener> listeners;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;
    private final AVector<InjectedProperty> injectedProperties;
    private final int defaultFetchSize;

    public AQueryImpl(Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                      AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier,
                      AVector<InjectedProperty> injectedProperties, int defaultFetchSize) {
        this.rowClass = cls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
        this.injectedProperties = injectedProperties;
        this.defaultFetchSize = defaultFetchSize;
    }

    protected AQueryImpl<T> build(Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                                  AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier,
                                  AVector<InjectedProperty> injectedProperties, int defaultFetchSize) {
        return new AQueryImpl<>(cls, sql, primTypes, rowExtractor, listeners, defaultConnectionSupplier, injectedProperties, defaultFetchSize);
    }

    @Override public AQuery<T> withInjectedProperty(InjectedProperty injectedProperty) {
        if (injectedProperties.exists(p -> p.propertyName().equals(injectedProperty.propertyName()))) {
            throw new IllegalArgumentException("attempted to add a second injected property with name " + injectedProperty.propertyName());
        }

        return build(rowClass, sql, primTypes, rowExtractor, listeners, defaultConnectionSupplier, injectedProperties.append(injectedProperty), defaultFetchSize);
    }

    @Override public T single() {
        return single(defaultConnection());
    }

    private Connection defaultConnection() {
        return defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get();
    }

    @Override public T single(Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) {
                throw new NoSuchElementException("no result");
            }
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final T result = doExtract(conn, new LiveSqlRow(primTypes, rs), memento, false, injectedPropertyMementos(conn));
            if (rs.next()) {
                throw new IllegalStateException("more than one result row");
            }
            afterIteration(1);
            return result;
        }));
    }

    private Map<String, Object> injectedPropertyMementos(Connection conn) {
        if (injectedProperties.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Object> result = new HashMap<>();
        for (InjectedProperty<?> ip : injectedProperties) {
            result.put(ip.propertyName(), ip.mementoPerQuery(conn, rowClass, sql));
        }
        return result;
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
        catch (Throwable th) {
            listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
            AUnchecker.throwUnchecked(th);
            throw new RuntimeException("just for the compiler");
        }
    }

    @Override public AOption<T> optional() {
        return optional(defaultConnection());
    }

    @Override public AOption<T> optional(Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) {
                afterIteration(0);
                return AOption.empty();
            }
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final T result = doExtract(conn, new LiveSqlRow(primTypes, rs), memento, false, injectedPropertyMementos(conn));
            if (rs.next()) {
                throw new IllegalStateException("more than one result row");
            }
            afterIteration(1);
            return AOption.some(result);
        }));
    }

    @Override public AOption<T> first() {
        return first(defaultConnection());
    }

    @Override public AOption<T> first(Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) {
                afterIteration(0);
                return AOption.empty();
            }
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final T result = doExtract(conn, new LiveSqlRow(primTypes, rs), memento, false, injectedPropertyMementos(conn));
            afterIteration(1);
            return AOption.some(result);
        }));
    }

    @Override public <R,A> R collect(Collector<T, A, R> collector) {
        return collect(defaultConnection(), collector);
    }
    @Override public <R,A> R collect(Connection conn, Collector<T, A, R> collector) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            final A acc = collector.supplier().get();
            int count = 0;

            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final Map<String, Object> injectedPropsMementos = injectedPropertyMementos(conn);
            final LiveSqlRow row = new LiveSqlRow(primTypes, rs);
            while (rs.next()) {
                final T el = doExtract(conn, row, memento, false, injectedPropsMementos);
                collector.accumulator().accept(acc, el);
                count += 1;
            }
            afterIteration(count);
            return collector.finisher().apply(acc);
        }));
    }
    @Override public AList<T> list() {
        return list(defaultConnection());
    }

    @Override public AList<T> list(Connection conn) {
        return collect(conn, AVector.streamCollector());
    }

    private Map<String, Object> injectedPropsValuesForRow(Connection conn, SqlRow currentRow, Map<String, Object> injectedPropsMementos) {
        if (injectedProperties.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Object> result = new HashMap<>();
        for (InjectedProperty ip : injectedProperties) {
            //noinspection unchecked
            final AOption<Object> optValue = ip.value(conn, currentRow, injectedPropsMementos.get(ip.propertyName()));
            optValue.forEach(o -> result.put(ip.propertyName(), o));
        }
        return result;
    }

    private T doExtract(Connection conn, LiveSqlRow row, Object memento, boolean isStreaming, Map<String, Object> injectedPropsMementos) throws SQLException {
        final Map<String, Object> injectedPropsValues = injectedPropsValuesForRow(conn, row, injectedPropsMementos);
        return rowExtractor.fromSql(rowClass, primTypes, row, memento, isStreaming, injectedPropsValues);
    }

    /**
     * This method returns a {@link Stream} of mapped rows. This method is for advanced use and
     * special cases. <p>
     *
     * TODO forEach / list / collect
     *
     * The underlying database resources are closed on a best effort basis, but there
     * are <b>no guarantees</b> that they will be released automatically. Code using this method
     * <b>must</b> close the returned stream.
     */
    @Override public Stream<T> stream() {
        return stream(defaultConnection());
    }
    @Override public Stream<T> stream(Connection conn) {
        return stream(conn, defaultFetchSize);
    }
    @Override public Stream<T> stream(int fetchSize) {
        return stream(defaultConnection(), fetchSize);
    }
    @Override public Stream<T> stream(Connection conn, int fetchSize) {
        final ResultSetSpliterator rss = new ResultSetSpliterator(conn, fetchSize);
        return StreamSupport.stream(rss, false)
                .onClose(rss::close);
    }

    @Override public void forEach(Consumer<T> consumer) {
        forEach(defaultConnection(), consumer);
    }
    @Override public void forEach(Connection conn, Consumer<T> consumer) {
        forEach(conn, defaultFetchSize, consumer);
    }
    @Override public void forEach(int fetchSize, Consumer<T> consumer) {
        forEach(defaultConnection(), fetchSize, consumer);
    }
    @Override public void forEach(Connection conn, int fetchSize, Consumer<T> consumer) {
        try (Stream<T> s = stream(conn, fetchSize)) {
            s.forEach(consumer);
        }

    }

    @Override public void forEachWithRowAccess(BiConsumer<T, SqlRow> consumer) {
        forEachWithRowAccess(defaultConnection(), consumer);
    }
    @Override public void forEachWithRowAccess(Connection conn, BiConsumer<T, SqlRow> consumer) {
        forEachWithRowAccess(conn, defaultFetchSize, consumer);
    }
    @Override public void forEachWithRowAccess(int fetchSize, BiConsumer<T, SqlRow> consumer) {
        forEachWithRowAccess(defaultConnection(), fetchSize, consumer);
    }
    @Override public void forEachWithRowAccess(Connection conn, int fetchSize, BiConsumer<T, SqlRow> consumer) {
        final ResultSetSpliterator rss = new ResultSetSpliterator(conn, fetchSize);
        try (Stream<T> s = StreamSupport.stream(rss, false).onClose(rss::close)) {
            s.forEach(el -> consumer.accept(el, rss.getCurrentRow()));
        }
    }

    //TODO fail if injected properties are present
    //TODO special handling - 'raw' --> ohne zusätzliches Mapping, nur Wrapper --> rowExtractor == RawRowExtractor.INSTANCE

    private class ResultSetSpliterator implements Spliterator<T> {
        private final Connection conn;
        private final int fetchSize;
        private PreparedStatement ps;
        private ResultSet rs;
        private LiveSqlRow row;
        private Object memento;
        private Map<String, Object> injectedPropsMementos;
        private int numRows = 0;

        private boolean started = false;
        private boolean closed = false;

        ResultSetSpliterator(Connection conn, int fetchSize) {
            this.conn = conn;
            this.fetchSize = fetchSize;
        }

        SqlRow getCurrentRow() {
            if (closed) {
                throw new IllegalStateException("stream is closed");
            }
            if (!started) {
                throw new IllegalStateException("stream is not started");
            }
            return row;
        }

        private void startLazily() {
            if(started) {
                return;
            }
            started = true;

            try {
                listeners.forEach(l -> l.onBeforeQuery(sql, rowClass));
                ps = conn.prepareStatement(sql.getSql());
                ps.setFetchSize(fetchSize);
            }
            catch (Throwable th) {
                listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
                AUnchecker.throwUnchecked(th);
                throw new Error(); // for the compiler
            }
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                rs = ps.executeQuery();
                row = new LiveSqlRow(primTypes, rs);
                listeners.reverseIterator().forEachRemaining(SqlEngineEventListener::onAfterQueryExecution);
                memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, true);
                injectedPropsMementos = injectedPropertyMementos(conn);
            }
            catch (Throwable th) {
                releaseResources();
                listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
                AUnchecker.throwUnchecked(th);
                throw new Error(); // for the compiler
            }
        }

        @Override public boolean tryAdvance(Consumer<? super T> action) {
            startLazily();

            try {
                if (!rs.next()) {
                    close();
                    return false;
                }
                numRows += 1;
                action.accept(doExtract(conn, row, memento, true, injectedPropsMementos));
                return true;
            }
            catch (Throwable th) {
                listeners.reverseIterator().forEachRemaining(l -> l.onFailed(th));
                releaseResources();
                AUnchecker.throwUnchecked(th);
                return false; // dead code - for the compiler
            }
        }

        private void close() {
            releaseResources();
            if (!closed) {
                //only call this once
                afterIteration(numRows);
            }
            closed = true;
        }

        private void releaseResources() {
            SqlHelper.closeQuietly(rs);
            SqlHelper.closeQuietly(ps);
            rs = null;
            ps = null;
        }

        @Override public Spliterator<T> trySplit() {
            return null;
        }
        @Override public long estimateSize() {
            return Long.MAX_VALUE;
        }
        @Override public int characteristics() {
            return ORDERED;
        }
    }
}

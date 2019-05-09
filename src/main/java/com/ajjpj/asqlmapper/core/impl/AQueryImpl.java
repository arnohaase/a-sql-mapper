package com.ajjpj.asqlmapper.core.impl;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
import com.ajjpj.asqlmapper.core.common.LiveSqlRow;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.common.SqlStream;
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

    public AQueryImpl (Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor,
                       AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier, AVector<InjectedProperty> injectedProperties) {
        this.rowClass = cls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
        this.injectedProperties = injectedProperties;
    }

    @Override public AQuery<T> withInjectedProperty (InjectedProperty injectedProperty) {
        return new AQueryImpl<>(rowClass, sql, primTypes, rowExtractor, listeners, defaultConnectionSupplier, injectedProperties.append(injectedProperty));
    }

    @Override public T single () {
        return single(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public T single (Connection conn) {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) throw new IllegalStateException("no result");
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final T result = doExtract(conn, new LiveSqlRow(primTypes, rs), memento, false, injectedPropertyMementos(conn));
            if (rs.next()) throw new IllegalStateException("more than one result row");
            afterIteration(1);
            return result;
        }));
    }

    private Map<String, Object> injectedPropertyMementos(Connection conn) {
        if(injectedProperties.isEmpty())
            return Collections.emptyMap();

        final Map<String,Object> result = new HashMap<>();
        for(InjectedProperty<?> ip: injectedProperties) {
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
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final T result = doExtract(conn, new LiveSqlRow(primTypes, rs), memento, false, injectedPropertyMementos(conn));
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
            final Object memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, false);
            final Map<String,Object> injectedPropsMementos = injectedPropertyMementos(conn);
            final LiveSqlRow row = new LiveSqlRow(primTypes, rs);
            while (rs.next()) builder.add(doExtract(conn, row, memento, false, injectedPropsMementos));
            final AList<T> result = builder.build();
            afterIteration(result.size());
            return result;
        }));
    }

    private Map<String,Object> injectedPropsValuesForRow(Connection conn, SqlRow currentRow, Map<String,Object> injectedPropsMementos) {
        if(injectedProperties.isEmpty())
            return Collections.emptyMap();

        final Map<String,Object> result = new HashMap<>();
        for(InjectedProperty ip: injectedProperties) {
            //noinspection unchecked
            final AOption<Object> optValue = ip.value(conn, currentRow, injectedPropsMementos.get(ip.propertyName()));
            optValue.forEach(o -> result.put(ip.propertyName(), o));
        }
        return result;
    }

    private T doExtract(Connection conn, LiveSqlRow row, Object memento, boolean isStreaming, Map<String,Object> injectedPropsMementos) throws SQLException {
        final Map<String,Object> injectedPropsValues = injectedPropsValuesForRow(conn, row, injectedPropsMementos);
        return rowExtractor.fromSql(rowClass, primTypes, row, memento, isStreaming, injectedPropsValues);
    }

    @Override public Stream<T> stream () {
        return stream(defaultConnectionSupplier
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get());
    }

    @Override public Stream<T> stream (Connection conn) {
        return StreamSupport.stream(() -> new ResultSetSpliterator(conn), Spliterator.ORDERED, false);
    }

    /**
     * This method returns a {@link SqlStream}, i.e. a {@link Stream} that provides access to the current row
     *  during processing.<p>
     *
     * This goes against the concept of {@link Stream}, but it allows handling special cases where structural
     *  information of objects is extracted from result columns that are not mapped to the resulting bean.
     */
    public SqlStream<T> streamWithRowAccess (Connection conn) {
        final ResultSetSpliterator spliterator = new ResultSetSpliterator(conn);
        final Stream<T> inner = StreamSupport.stream(() -> spliterator, Spliterator.ORDERED, false);

        //noinspection unchecked
        return (SqlStream<T>) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{SqlStream.class}, (proxy, method, args) -> {
            if("currentRow".equals(method.getName()))
                return spliterator.row;
            try {
                return method.invoke(inner, args);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private class ResultSetSpliterator implements Spliterator<T> {
        private final Connection conn;
        private final PreparedStatement ps;
        private final ResultSet rs;
        final LiveSqlRow row;
        private final Object memento;
        private final Map<String,Object> injectedPropsMementos;
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
                row = new LiveSqlRow(primTypes, rs);
                listeners.reverseIterator().forEachRemaining(SqlEngineEventListener::onAfterQueryExecution);
                memento = rowExtractor.mementoPerQuery(rowClass, primTypes, rs, true);
                injectedPropsMementos = injectedPropertyMementos(conn);
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
                action.accept(doExtract(conn, row, memento, true, injectedPropsMementos));
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

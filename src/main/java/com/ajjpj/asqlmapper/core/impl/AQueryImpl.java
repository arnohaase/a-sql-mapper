package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.acollections.util.AUnchecker;
import com.ajjpj.asqlmapper.core.AQuery;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.SqlSnippet;

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
    private final Class<T> cls;
    private final SqlSnippet sql;
    private final PrimitiveTypeRegistry primTypes;
    private final RowExtractor rowExtractor;

    public AQueryImpl (Class<T> cls, SqlSnippet sql, PrimitiveTypeRegistry primTypes, RowExtractor rowExtractor) {
        this.cls = cls;
        this.sql = sql;
        this.primTypes = primTypes;
        this.rowExtractor = rowExtractor;
    }

    @Override public T single (Connection conn) throws SQLException {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) throw new IllegalStateException("no result");
            final T result = rowExtractor.fromSql(cls, primTypes, rs, rowExtractor.mementoPerQuery(cls, primTypes, rs));
            if (rs.next()) throw new IllegalStateException("more than one result row");
            return result;
        }));
    }

    private <X> X doQuery(Connection conn, Function<ResultSet, X> resultHandler) throws SQLException {
        try (final PreparedStatement ps = conn.prepareStatement(sql.getSql())) {
            SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
            final ResultSet rs = ps.executeQuery();
            return resultHandler.apply(rs);
        }
    }

    @Override public AOption<T> optional (Connection conn) throws SQLException {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            if (!rs.next()) return AOption.empty();
            final T result = rowExtractor.fromSql(cls, primTypes, rs, rowExtractor.mementoPerQuery(cls, primTypes, rs));
            if (rs.next()) throw new IllegalStateException("more than one result row");
            return AOption.some(result);
        }));
    }

    @Override public AList<T> list (Connection conn) throws SQLException {
        return doQuery(conn, rs -> executeUnchecked(() -> {
            final AVector.Builder<T> result = AVector.builder();
            final Object memento = rowExtractor.mementoPerQuery(cls, primTypes, rs);
            while (rs.next()) result.add(rowExtractor.fromSql(cls, primTypes, rs, memento));
            return result.build();
        }));
    }

    @Override public Stream<T> stream (Connection conn) {
        return StreamSupport.stream(() -> new ResultSetSpliterator(conn), Spliterator.ORDERED, false);
    }

    private class ResultSetSpliterator implements Spliterator<T> {
        private final PreparedStatement ps;
        private final ResultSet rs;
        private final Object memento;

        ResultSetSpliterator(Connection conn) {
            try {
                ps = conn.prepareStatement(sql.getSql());
            }
            catch (Throwable th) {
                AUnchecker.throwUnchecked(th);
                throw new Error(); // for the compiler
            }
            try {
                SqlHelper.bindParameters(ps, sql.getParams(), primTypes);
                rs = ps.executeQuery();
                memento = rowExtractor.mementoPerQuery(cls, primTypes, rs);
            }
            catch (Throwable th) {
                SqlHelper.closeQuietly(ps);
                AUnchecker.throwUnchecked(th);
                throw new Error(); // for the compiler
            }
        }

        @Override public boolean tryAdvance (Consumer<? super T> action) {
            try {
                if (!rs.next()) return false;
                action.accept(rowExtractor.fromSql(cls, primTypes, rs, memento));
                return true;
            }
            catch(Throwable th) {
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

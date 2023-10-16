package com.ajjpj.asqlmapper.core;

import java.sql.Connection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.injectedproperties.InjectedProperty;

/**
 * represents a SELECT statement
 */
public interface AQuery<T> {
    /**
     * This method adds values to the result that do not come from this query but from an external source,
     * hence the name 'injected'. One typical source of injected properties is separate queries for to-one
     * or to-many queries; {@link com.ajjpj.asqlmapper.mapper.SqlMapper} has methods to create them
     * conveniently ({@link com.ajjpj.asqlmapper.mapper.SqlMapper#toOne(String)},
     * {@link com.ajjpj.asqlmapper.mapper.SqlMapper#oneToMany(String)},
     * {@link com.ajjpj.asqlmapper.mapper.SqlMapper#manyToMany(String)}).
     */
    AQuery<T> withInjectedProperty(InjectedProperty injectedProperty);

    T single(Connection conn);
    T single();
    AOption<T> optional(Connection conn);
    AOption<T> optional();
    AOption<T> first(Connection conn);
    AOption<T> first();
    AList<T> list(Connection conn);
    AList<T> list();

    <R,A> R collect(Connection conn, Collector<T,A,R> collector);
    <R,A> R collect(Collector<T,A,R> collector);

    //TODO documentation
    Stream<T> stream(Connection conn);
    Stream<T> stream();
    Stream<T> stream(int fetchSize);
    Stream<T> stream(Connection conn, int fetchSize);

    void forEach(Connection conn, Consumer<T> consumer);
    void forEach(Consumer<T> consumer);
    void forEach(int fetchSize, Consumer<T> consumer);
    void forEach(Connection conn, int fetchSize, Consumer<T> consumer);

    void forEachWithRowAccess(Connection conn, BiConsumer<T, SqlRow> consumer);
    void forEachWithRowAccess(BiConsumer<T, SqlRow> consumer);
    void forEachWithRowAccess(int fetchSize, BiConsumer<T, SqlRow> consumer);
    void forEachWithRowAccess(Connection conn, int fetchSize, BiConsumer<T, SqlRow> consumer);
}

package com.ajjpj.asqlmapper.core;

import java.sql.Connection;
import java.util.stream.Stream;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.common.SqlStream;
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
    AList<T> list(Connection conn);
    AList<T> list();
    Stream<T> stream(Connection conn);
    Stream<T> stream();
}

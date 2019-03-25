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

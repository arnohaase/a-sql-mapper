package com.ajjpj.asqlmapper.core;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.provided.ProvidedValues;

import java.sql.Connection;
import java.util.stream.Stream;


/**
 * represents a SELECT statement
 */
public interface AQuery<T> {
    T single(Connection conn);
    T single();
    AOption<T> optional(Connection conn);
    AOption<T> optional();
    AList<T> list(Connection conn);
    AList<T> list();
    Stream<T> stream(Connection conn);
    Stream<T> stream();

    AQuery<T> withPropertyValues (String propName, String referencedColumnName, ProvidedValues providedValues);
}

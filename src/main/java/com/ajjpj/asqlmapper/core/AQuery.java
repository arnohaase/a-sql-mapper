package com.ajjpj.asqlmapper.core;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.util.AOption;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;


/**
 * represents a SELECT statement
 */
public interface AQuery<T> {
    T single(Connection conn) throws SQLException;
    AOption<T> optional(Connection conn) throws SQLException;
    AList<T> list(Connection conn) throws SQLException;
    Stream<T> stream(Connection conn);
}

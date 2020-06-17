package com.ajjpj.asqlmapper.core;

import com.ajjpj.acollections.AList;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * special handling for INSERT statements with generated columns
 *
 * @param <T> the type containing the generated column(s) - typically there is only one generated column, and this is its scalar type
 */
interface AInsert<T> {
    T executeSingle(Connection conn);
    T executeSingle();
    AList<T> executeMulti(Connection conn);
    AList<T> executeMulti();
}

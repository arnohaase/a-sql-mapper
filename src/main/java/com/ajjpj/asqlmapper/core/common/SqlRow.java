package com.ajjpj.asqlmapper.core.common;

import com.ajjpj.acollections.immutable.AVector;

public interface SqlRow {
    /**
     * @return a {@link DetachedSqlRow} with this SqlRow's data.
     */
    DetachedSqlRow detach();

    default int numColumns() {
        return columnNames().size();
    }
    AVector<String> columnNames();

    <T> T get(Class<T> cls, String columnName);
    Object get(String columnName);

    default <T> T get(Class<T> cls, int idx) {
        return get(cls, columnNames().get(idx));
    }
    default Object get(int idx) {
        return get(columnNames().get(idx));
    }

    default String getString(String columnName) {
        return get(String.class, columnName);
    }
    default String getString(int idx) {
        return get(String.class, idx);
    }

    default Integer getInt(String columnName) {
        return get(Integer.class, columnName);
    }
    default Integer getInt(int idx) {
        return get(Integer.class, idx);
    }

    default Long getLong(String columnName) {
        return get(Long.class, columnName);
    }
    default Long getLong(int idx) {
        return get(Long.class, idx);
    }
}

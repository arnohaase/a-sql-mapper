package com.ajjpj.asqlmapper.mapper.beans.primarykey;

import com.ajjpj.acollections.util.AOption;

import java.sql.Connection;


/**
 * strategy instances can be stateful, e.g. by pre-fetching primary keys from a database.
 */
public interface PkStrategy {
    default boolean isAutoIncrement() {
        return false;
    }

    /**
     * @return empty() for 'manually set'
     */
    AOption<Object> newPrimaryKey(Connection conn);
}

package com.ajjpj.asqlmapper.mapper.beans.primarykey;

import com.ajjpj.acollections.util.AOption;

import java.sql.Connection;

public class AutoIncrementPkStrategy implements PkStrategy {
    @Override public boolean isAutoIncrement () {
        return true;
    }

    @Override public AOption<Object> newPrimaryKey (Connection conn) {
        throw new UnsupportedOperationException("should never be called for autoincrement primary keys");
    }
}

package com.ajjpj.asqlmapper.mapper.beans.primarykey;

import com.ajjpj.acollections.util.AOption;

import java.sql.Connection;

public class ManualPkStrategy implements PkStrategy {
    @Override public AOption<Object> newPrimaryKey (Connection conn) {
        return AOption.empty();
    }
}

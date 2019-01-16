package com.ajjpj.asqlmapper.mapper.beans.primarykey;

import com.ajjpj.acollections.util.AOption;

import java.sql.Connection;
import java.util.UUID;


public class RandomUuidPkStrategy implements PkStrategy {
    @Override public AOption<Object> newPrimaryKey (Connection conn) {
        return AOption.some(UUID.randomUUID());
    }
}

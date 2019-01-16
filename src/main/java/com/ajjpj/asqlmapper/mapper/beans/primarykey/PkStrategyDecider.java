package com.ajjpj.asqlmapper.mapper.beans.primarykey;

import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.sql.Connection;

public interface PkStrategyDecider {
    PkStrategy pkStrategy(Connection conn, Class<?> beanType, TableMetaData tableMetaData);
}

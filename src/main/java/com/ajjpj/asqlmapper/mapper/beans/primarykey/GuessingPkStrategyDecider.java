package com.ajjpj.asqlmapper.mapper.beans.primarykey;

import com.ajjpj.asqlmapper.mapper.schema.TableMetaData;

import java.sql.Connection;


public class GuessingPkStrategyDecider implements PkStrategyDecider {
    @Override public PkStrategy pkStrategy (Connection conn, Class<?> beanType, TableMetaData tableMetaData) {
        if (tableMetaData.pkColumns().size() == 1 && tableMetaData.pkColumns().head().isAutoIncrement())
            return new AutoIncrementPkStrategy();

        //TODO
        return new ManualPkStrategy();
    }
}

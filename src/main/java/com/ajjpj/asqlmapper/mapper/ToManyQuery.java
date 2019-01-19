package com.ajjpj.asqlmapper.mapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;


public interface ToManyQuery<K,R>  {
    Map<K,R> execute(Connection conn) throws SQLException;
    ToManyQuery<K,R> withPropertyValues (String propName, Map<Object,Object> providedValues);
}

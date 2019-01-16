package com.ajjpj.asqlmapper.core.impl;

import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public class SqlHelper {
    public static void bindParameters(PreparedStatement ps, List<?> params, PrimitiveTypeRegistry primTypes) throws SQLException {
        int idx = 1;
        for(Object o: params) {
            ps.setObject(idx, primTypes.toSql(o));
            idx += 1;
        }
    }

    public static void closeQuietly(AutoCloseable cl) {
        try {
            if (cl != null) cl.close();
        }
        catch(Exception exc) {
            //TODO log?
        }
    }
}

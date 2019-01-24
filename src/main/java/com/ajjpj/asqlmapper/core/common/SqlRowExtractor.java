package com.ajjpj.asqlmapper.core.common;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class SqlRowExtractor implements RowExtractor {
    public static final SqlRowExtractor INSTANCE = new SqlRowExtractor();

    private SqlRowExtractor () {
    }

    @Override public boolean canHandle (Class<?> cls) {
        return cls == SqlRow.class;
    }

    @Override public Object mementoPerQuery (Class<?> cls, PrimitiveTypeRegistry primTypes, ResultSet rs) throws SQLException {
        final ResultSetMetaData rsMeta = rs.getMetaData();
        final AVector.Builder<String> builder = AVector.builder();
        for (int i=1; i<= rsMeta.getColumnCount(); i++) {
            builder.add(rsMeta.getColumnName(i));
        }
        return builder.build();
    }

    @Override public <T> T fromSql (Connection conn, Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) {
        //noinspection unchecked
        return (T) new SqlRow(rs, (AVector<String>) mementoPerQuery, primTypes);
    }
}

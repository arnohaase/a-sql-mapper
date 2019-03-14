package com.ajjpj.asqlmapper.core.common;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;
import com.ajjpj.asqlmapper.core.provided.ProvidedProperties;


public class RawRowExtractor implements RowExtractor {
    private static final Logger log = LoggerFactory.getLogger(RawRowExtractor.class);

    public static final RawRowExtractor INSTANCE = new RawRowExtractor();

    private RawRowExtractor () {
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

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, ProvidedProperties providedProperties) throws SQLException {
        if(providedProperties.nonEmpty()) {
            log.warn("provided properties ignored for raw row extractor");
        }
        //noinspection unchecked
        return (T) new SqlRow(rs, (AVector<String>) mementoPerQuery, primTypes);
    }
}

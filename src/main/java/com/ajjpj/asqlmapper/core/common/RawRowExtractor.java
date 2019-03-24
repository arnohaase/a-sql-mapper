package com.ajjpj.asqlmapper.core.common;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    @Override public Object mementoPerQuery (Class<?> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, boolean isStreaming) throws SQLException {
        return new LiveSqlRow(primTypes, rs);
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery, boolean isStreaming, ProvidedProperties providedProperties) {
        if(providedProperties.nonEmpty()) {
            log.warn("provided properties ignored for raw row extractor");
        }
        final SqlRow result = (SqlRow) mementoPerQuery;
        //noinspection unchecked
        return (T)(isStreaming ? result : result.detach());
    }
}

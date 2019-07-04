package com.ajjpj.asqlmapper.core.common;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;


public class RawRowExtractor implements RowExtractor {
    private static final Logger log = LoggerFactory.getLogger(RawRowExtractor.class);

    public static final RawRowExtractor INSTANCE = new RawRowExtractor();

    private RawRowExtractor () {
    }

    @Override public boolean canHandle (Class<?> cls) {
        return cls == SqlRow.class;
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, SqlRow row, Object mementoPerQuery, boolean isStreaming, Map<String,Object> injectedPropsValues) {
        if(! injectedPropsValues.isEmpty()) {
            log.warn("provided properties ignored for raw row extractor");
        }
        //noinspection unchecked
        return (T)(isStreaming ? row : row.detach());
    }
}

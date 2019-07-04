package com.ajjpj.asqlmapper.core.common;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;


public class ScalarRowExtractor implements RowExtractor {
    private static final Logger log = LoggerFactory.getLogger(ScalarRowExtractor.class);

    public static final ScalarRowExtractor LONG_EXTRACTOR = new ScalarRowExtractor(Long.class);
    public static final ScalarRowExtractor INT_EXTRACTOR = new ScalarRowExtractor(Integer.class);
    public static final ScalarRowExtractor STRING_EXTRACTOR = new ScalarRowExtractor(String.class);
    public static final ScalarRowExtractor UUID_EXTRACTOR = new ScalarRowExtractor(UUID.class);
    public static final ScalarRowExtractor BOOLEAN_EXTRACTOR = new ScalarRowExtractor(Boolean.class);
    public static final ScalarRowExtractor DOUBLE_EXTRACTOR = new ScalarRowExtractor(Double.class);
    public static final ScalarRowExtractor BIG_DECIMAL_EXTRACTOR = new ScalarRowExtractor(BigDecimal.class);

    private final Class<?> cls;

    public ScalarRowExtractor (Class<?> cls) {
        this.cls = cls;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return this.cls.isAssignableFrom(cls);
    }

    @Override public <T> T fromSql (Class<T> cls, PrimitiveTypeRegistry primTypes, SqlRow row, Object mementoPerQuery, boolean isStreaming,
                                    Map<String,Object> injectedPropsValues) throws SQLException {
        if (! injectedPropsValues.isEmpty()) {
            log.warn("provided properties ignored for scalar queries");
        }
        return row.get(cls, 0);
    }
}

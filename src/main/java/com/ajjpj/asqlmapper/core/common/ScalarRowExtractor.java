package com.ajjpj.asqlmapper.core.common;

import com.ajjpj.asqlmapper.core.PrimitiveTypeRegistry;
import com.ajjpj.asqlmapper.core.RowExtractor;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class ScalarRowExtractor<T> implements RowExtractor {
    public static final ScalarRowExtractor<Long> LONG_EXTRACTOR = new ScalarRowExtractor<>(Long.class);
    public static final ScalarRowExtractor<Integer> INT_EXTRACTOR = new ScalarRowExtractor<>(Integer.class);
    public static final ScalarRowExtractor<String> STRING_EXTRACTOR = new ScalarRowExtractor<>(String.class);
    public static final ScalarRowExtractor<UUID> UUID_EXTRACTOR = new ScalarRowExtractor<>(UUID.class);
    public static final ScalarRowExtractor<Boolean> BOOLEAN_EXTRACTOR = new ScalarRowExtractor<>(Boolean.class);
    public static final ScalarRowExtractor<Double> DOUBLE_EXTRACTOR = new ScalarRowExtractor<>(Double.class);
    public static final ScalarRowExtractor<BigDecimal> BIG_DECIMAL_EXTRACTOR = new ScalarRowExtractor<>(BigDecimal.class);

    private final Class<T> cls;

    public ScalarRowExtractor (Class<T> cls) {
        this.cls = cls;
    }

    @Override public boolean canHandle (Class<?> cls) {
        return this.cls.isAssignableFrom(cls);
    }

    @Override public <U> U fromSql (Connection conn, Class<U> cls, PrimitiveTypeRegistry primTypes, ResultSet rs, Object mementoPerQuery) throws SQLException {
        return primTypes.fromSql(cls, rs.getObject(1));
    }
}

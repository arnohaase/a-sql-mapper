package com.ajjpj.asqlmapper;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.*;
import com.ajjpj.asqlmapper.core.common.ScalarRowExtractor;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.common.SqlRowExtractor;
import com.ajjpj.asqlmapper.core.impl.*;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.function.Function;

/**
 * not necessary but convenient
 * immutable, mutators return modified copy
 */
public class ASqlEngine {
    private final PrimitiveTypeRegistry primTypes;
    private final AOption<String> optDefaultPkName;
    private final CanHandleRegistry<RowExtractor> rowExtractorRegistry;

    public static ASqlEngine create() {
        return create (PrimitiveTypeRegistry.defaults());
    }

    public static ASqlEngine create(PrimitiveTypeRegistry primTypes) {
        return new ASqlEngine(primTypes, AOption.none(), CanHandleRegistry.empty());
    }

    private ASqlEngine (PrimitiveTypeRegistry primTypes, AOption<String> optDefaultPkName, CanHandleRegistry<RowExtractor> rowExtractorRegistry) {
        this.primTypes = primTypes;
        this.optDefaultPkName = optDefaultPkName;
        this.rowExtractorRegistry = rowExtractorRegistry;
    }

    public PrimitiveTypeRegistry primitiveTypeRegistry() {
        return primTypes;
    }

    public RowExtractor rowExtractorFor (Class<?> cls) {
        return rowExtractorRegistry
                .handlerFor(cls)
                .orElseThrow(() -> new IllegalArgumentException("no row extractor registered for " + cls + " - pass in a RowExtractor instance explicitly or register it"));
    }

    //--------------------------- generic update statements, i.e. statements not returning a result set

    public AUpdate update(SqlSnippet sql) {
        return new AUpdateImpl(sql, primTypes);
    }

    public AUpdate update(String sql, Object... params) {
        return new AUpdateImpl(SqlSnippet.sql(sql, params), primTypes);
    }

    //--------------------------- insert statements, i.e. statements returning a generated primary key

    private String defaultPkName() {
        return optDefaultPkName.orElseThrow(() -> new IllegalStateException("no default PK name was defined - call 'ASqlEngine.withDefaultPkName()' to set it"));
    }

    public AInsert<UUID> insertUuidPk(String sql, Object... params) {
        return insertUuidPk(SqlSnippet.sql(sql, params), defaultPkName());
    }
    public AInsert<String> insertStringPk(String sql, Object... params) {
        return insertStringPk(SqlSnippet.sql(sql, params), defaultPkName());
    }
    public AInsert<Integer> insertIntegerPk(String sql, Object... params) {
        return insertIntegerPk(SqlSnippet.sql(sql, params), defaultPkName());
    }
    public AInsert<Long> insertLongPk(String sql, Object... params) {
        return insertLongPk(SqlSnippet.sql(sql, params), defaultPkName());
    }
    public <T> AInsert<T> insertSingleColPk (Class<T> pkType, String sql, Object... params) {
        return insertSingleColPk(pkType, SqlSnippet.sql(sql, params), defaultPkName());
    }

    public AInsert<UUID> insertUuidPk(SqlSnippet sql, String colName) {
        return insert(UUID.class, ScalarRowExtractor.UUID_EXTRACTOR, sql, AVector.of(colName));
    }
    public AInsert<String> insertStringPk(SqlSnippet sql, String colName) {
        return insert(String.class, ScalarRowExtractor.STRING_EXTRACTOR, sql, AVector.of(colName));
    }
    public AInsert<Integer> insertIntegerPk(SqlSnippet sql, String colName) {
        return insert(Integer.class, ScalarRowExtractor.INT_EXTRACTOR, sql, AVector.of(colName));
    }
    public AInsert<Long> insertLongPk(SqlSnippet sql, String colName) {
        return insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, sql, AVector.of(colName));
    }
    public <T> AInsert<T> insertSingleColPk (Class<T> pkType, SqlSnippet sql, String colName) {
        return insert(pkType, new ScalarRowExtractor<>(pkType), sql, colName);
    }

    public <T> AInsert<T> insert(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, String colName1, String... colNames) {
        return insert(pkType, rowExtractor, sql, AVector.<String>builder().add(colName1).addAll(colNames).build());
    }
    public <T> AInsert<T> insert(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, AVector<String> colNames) { //TODO colNames varargs? works without?
        return new AInsertImpl<>(pkType, sql, primTypes, rowExtractor, colNames);
    }

    // -------------------------- select statements

    public <T> AQuery<T> scalarQuery(Class<T> columnType, SqlSnippet sql) {
        return new AQueryImpl<>(columnType, sql, primTypes, new ScalarRowExtractor<>(columnType));
    }
    public <T> AQuery<T> scalarQuery(Class<T> columnType, String sql, Object... params) {
        return scalarQuery(columnType, SqlSnippet.sql(sql, params));
    }

    public AQuery<Long> longQuery(SqlSnippet sql) {
        return new AQueryImpl<>(Long.class, sql, primTypes, ScalarRowExtractor.LONG_EXTRACTOR);
    }
    public AQuery<Long> longQuery(String sql, Object... params) {
        return longQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Integer> intQuery(SqlSnippet sql) {
        return new AQueryImpl<>(Integer.class, sql, primTypes, ScalarRowExtractor.INT_EXTRACTOR);
    }
    public AQuery<Integer> intQuery(String sql, Object... params) {
        return intQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<String> stringQuery(SqlSnippet sql) {
        return new AQueryImpl<>(String.class, sql, primTypes, ScalarRowExtractor.STRING_EXTRACTOR);
    }
    public AQuery<String> stringQuery(String sql, Object... params) {
        return stringQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Double> doubleQuery(SqlSnippet sql) {
        return new AQueryImpl<>(Double.class, sql, primTypes, ScalarRowExtractor.DOUBLE_EXTRACTOR);
    }
    public AQuery<Double> doubleQuery(String sql, Object... params) {
        return doubleQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<BigDecimal> bigDecimalQuery (SqlSnippet sql) {
        return new AQueryImpl<>(BigDecimal.class, sql, primTypes, ScalarRowExtractor.BIG_DECIMAL_EXTRACTOR);
    }
    public AQuery<BigDecimal> bigDecimalQuery (String sql, Object... params) {
        return bigDecimalQuery(SqlSnippet.sql(sql, params));
    }


    public AQuery<SqlRow> rawQuery(SqlSnippet sql) {
        return new AQueryImpl<>(SqlRow.class, sql, primTypes, SqlRowExtractor.INSTANCE);
    }
    public AQuery<SqlRow> rawQuery(String sql, Object... params) {
        return new AQueryImpl<>(SqlRow.class, SqlSnippet.sql(sql, params), primTypes, SqlRowExtractor.INSTANCE);
    }

    public <T> AQuery<T> query(Class<T> targetType, SqlSnippet sql) {
        return query(targetType, rowExtractorFor(targetType), sql);
    }

    public <T> AQuery<T> query(Class<T> targetType, String sql, Object... params) {
        return query(targetType, SqlSnippet.sql(sql, params));
    }

    public <T> AQuery<T> query(Class<T> cls, RowExtractor rowExtractor, SqlSnippet sql) { //TODO consistent ordering of parameters
        return new AQueryImpl<>(cls, sql, primTypes, rowExtractor);
    }

    //TODO tuples as query results


    //--------------------------- configuration

    public <T> ASqlEngine withRawTypeMapping(Class<T> jdbcType, Function<T, Object> rawMapping) {
        return new ASqlEngine(primTypes.withRawTypeMapping(jdbcType, rawMapping), optDefaultPkName, rowExtractorRegistry);
    }
    public ASqlEngine withPrimitiveHandler(PrimitiveTypeHandler handler) {
        return new ASqlEngine(primTypes.withHandler(handler), optDefaultPkName, rowExtractorRegistry);
    }

    public ASqlEngine withDefaultPkName(String pkName) {
        return new ASqlEngine(primTypes, AOption.of(pkName), rowExtractorRegistry);
    }

    public ASqlEngine withRowExtractor(RowExtractor rowExtractor) {
        return new ASqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry.withHandler(rowExtractor));
    }
}

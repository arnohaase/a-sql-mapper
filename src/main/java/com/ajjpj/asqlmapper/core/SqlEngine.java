package com.ajjpj.asqlmapper.core;

import static com.ajjpj.asqlmapper.core.SqlSnippet.concat;

import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.common.ScalarRowExtractor;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.common.RawRowExtractor;
import com.ajjpj.asqlmapper.core.impl.*;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * not necessary but convenient
 * immutable, mutators return modified copy
 */
public class SqlEngine {
    private final PrimitiveTypeRegistry primTypes;
    private final AOption<String> optDefaultPkName;
    private final CanHandleRegistry<RowExtractor> rowExtractorRegistry;
    private final AVector<SqlEngineEventListener> listeners;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;

    public static SqlEngine create() {
        return create (PrimitiveTypeRegistry.defaults());
    }

    public static SqlEngine create(PrimitiveTypeRegistry primTypes) {
        return new SqlEngine(primTypes, AOption.none(), CanHandleRegistry.empty(), AVector.empty(), AOption.empty());
    }

    private SqlEngine (PrimitiveTypeRegistry primTypes, AOption<String> optDefaultPkName, CanHandleRegistry<RowExtractor> rowExtractorRegistry,
                       AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier) {
        this.primTypes = primTypes;
        this.optDefaultPkName = optDefaultPkName;
        this.rowExtractorRegistry = rowExtractorRegistry;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
    }

    public PrimitiveTypeRegistry primitiveTypeRegistry() {
        return primTypes;
    }

    public RowExtractor rowExtractorFor (Class<?> cls) {
        final AOption<RowExtractor> registered = rowExtractorRegistry.handlerFor(cls);
        if (registered.isDefined()) return registered.get();

        if (primTypes.isPrimitiveType(cls)) return new ScalarRowExtractor(cls);

        throw new IllegalArgumentException("no row extractor registered for " + cls + " - pass in a RowExtractor instance explicitly or register it");
    }

    //--------------------------- generic update statements, i.e. statements not returning a result set

    public AUpdate update(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AUpdateImpl(concat(sql, moreSql), primTypes, listeners, defaultConnectionSupplier);
    }

    public AUpdate update(String sql, Object... params) {
        return new AUpdateImpl(SqlSnippet.sql(sql, params), primTypes, listeners, defaultConnectionSupplier);
    }

    //--------------------------- insert statements, i.e. statements returning a generated primary key

    private String defaultPkName() {
        return optDefaultPkName.orElseThrow(() -> new IllegalStateException("no default PK name was defined - call 'ASqlEngine.withDefaultPkName()' to set it"));
    }

    public AInsert<UUID> insertUuidPk(String sql, Object... params) {
        return insertUuidPk(SqlSnippet.sql(sql, params));
    }
    public AInsert<String> insertStringPk(String sql, Object... params) {
        return insertStringPk(SqlSnippet.sql(sql, params));
    }
    public AInsert<Integer> insertIntegerPk(String sql, Object... params) {
        return insertIntegerPk(SqlSnippet.sql(sql, params));
    }
    public AInsert<Long> insertLongPk(String sql, Object... params) {
        return insertLongPk(SqlSnippet.sql(sql, params));
    }
    public <T> AInsert<T> insertSingleColPk (Class<T> pkType, String sql, Object... params) {
        return insertSingleColPk(pkType, SqlSnippet.sql(sql, params));
    }

    public AInsert<UUID> insertUuidPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertUuidPkInCol(defaultPkName(), sql, moreSql);
    }
    public AInsert<String> insertStringPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertStringPkInCol(defaultPkName(), sql, moreSql);
    }
    public AInsert<Integer> insertIntegerPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertIntegerPkInCol(defaultPkName(), sql, moreSql);
    }
    public AInsert<Long> insertLongPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertLongPkInCol(defaultPkName(), sql, moreSql);
    }
    public <T> AInsert<T> insertSingleColPk (Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insertSingleColPkInCol(defaultPkName(), pkType, sql, moreSql);
    }

    public AInsert<UUID> insertUuidPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(UUID.class, ScalarRowExtractor.UUID_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public AInsert<String> insertStringPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(String.class, ScalarRowExtractor.STRING_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public AInsert<Integer> insertIntegerPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(Integer.class, ScalarRowExtractor.INT_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public AInsert<Long> insertLongPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public <T> AInsert<T> insertSingleColPkInCol(String colName, Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(pkType, new ScalarRowExtractor(pkType), concat(sql, moreSql), colName);
    }

    public <T> AInsert<T> insert(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, String colName1, String... colNames) {
        return insert(pkType, rowExtractor, sql, AVector.<String>builder().add(colName1).addAll(colNames).build());
    }
    public <T> AInsert<T> insert(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, AVector<String> colNames) {
        return new AInsertImpl<>(pkType, sql, primTypes, rowExtractor, colNames, listeners, defaultConnectionSupplier);
    }

    // -------------------------- select statements

    public <T> AQuery<T> scalarQuery(Class<T> columnType, SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(columnType, concat(sql, moreSql), primTypes, new ScalarRowExtractor(columnType), listeners, defaultConnectionSupplier, AVector.empty());
    }
    public <T> AQuery<T> scalarQuery(Class<T> columnType, String sql, Object... params) {
        return scalarQuery(columnType, SqlSnippet.sql(sql, params));
    }

    public AQuery<Long> longQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Long.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.LONG_EXTRACTOR, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<Long> longQuery(String sql, Object... params) {
        return longQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Integer> intQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Integer.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.INT_EXTRACTOR, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<Integer> intQuery(String sql, Object... params) {
        return intQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<String> stringQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(String.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.STRING_EXTRACTOR, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<String> stringQuery(String sql, Object... params) {
        return stringQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Double> doubleQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Double.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.DOUBLE_EXTRACTOR, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<Double> doubleQuery(String sql, Object... params) {
        return doubleQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<BigDecimal> bigDecimalQuery (SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(BigDecimal.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.BIG_DECIMAL_EXTRACTOR, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<BigDecimal> bigDecimalQuery (String sql, Object... params) {
        return bigDecimalQuery(SqlSnippet.sql(sql, params));
    }

    // TODO UUID query

    public AQuery<SqlRow> rawQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(SqlRow.class, concat(sql, moreSql), primTypes, RawRowExtractor.INSTANCE, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<SqlRow> rawQuery(String sql, Object... params) {
        return new AQueryImpl<>(SqlRow.class, SqlSnippet.sql(sql, params), primTypes, RawRowExtractor.INSTANCE, listeners, defaultConnectionSupplier, AVector.empty());
    }

    public <T> AQuery<T> query(Class<T> targetType, SqlSnippet sql, SqlSnippet... moreSql) {
        return query(targetType, rowExtractorFor(targetType), concat(sql, moreSql));
    }

    public <T> AQuery<T> query(Class<T> targetType, String sql, Object... params) {
        return query(targetType, SqlSnippet.sql(sql, params));
    }

    public <T> AQuery<T> query(Class<T> cls, RowExtractor rowExtractor, SqlSnippet sql, SqlSnippet... moreSql) { //TODO consistent ordering of parameters
        return new AQueryImpl<>(cls, concat(sql, moreSql), primTypes, rowExtractor, listeners, defaultConnectionSupplier, AVector.empty());
    }

    //TODO tuples as query results


    //--------------------------- configuration

    public AOption<Supplier<Connection>> defaultConnectionSupplier() {
        return defaultConnectionSupplier;
    }
    public Connection defaultConnection() {
        return defaultConnectionSupplier()
                .orElseThrow(() -> new IllegalStateException("no default connection supplier was configured"))
                .get();
    }

    public AVector<SqlEngineEventListener> listeners() {
        return listeners;
    }

    public <T> SqlEngine withRawTypeMapping(Class<T> jdbcType, Function<T, Object> rawMapping) {
        return new SqlEngine(primTypes.withRawTypeMapping(jdbcType, rawMapping), optDefaultPkName, rowExtractorRegistry, listeners, defaultConnectionSupplier);
    }
    public SqlEngine withPrimitiveHandler(PrimitiveTypeHandler handler) {
        return new SqlEngine(primTypes.withHandler(handler), optDefaultPkName, rowExtractorRegistry, listeners, defaultConnectionSupplier);
    }

    public SqlEngine withDefaultPkName(String pkName) {
        return new SqlEngine(primTypes, AOption.of(pkName), rowExtractorRegistry, listeners, defaultConnectionSupplier);
    }

    public SqlEngine withRowExtractor(RowExtractor rowExtractor) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry.withHandler(rowExtractor), listeners, defaultConnectionSupplier);
    }

    public SqlEngine withListener(SqlEngineEventListener listener) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry, listeners.append(listener), defaultConnectionSupplier);
    }

    /**
     * This is for using a connection from e.g. a ThreadLocal rather than requiring it to be passed in explicitly. It
     *  does <em>not</em> do any resource handling, so it should <em>not</em> be used to pull a new connection from
     *  a thread pool or data source - that would cause a resource leak because the connection would never be closed.
     */
    public SqlEngine withDefaultConnectionSupplier(Supplier<Connection> supp) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry, listeners, AOption.some(supp));
    }
}

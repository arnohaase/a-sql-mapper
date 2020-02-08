package com.ajjpj.asqlmapper.core;

import static com.ajjpj.asqlmapper.core.SqlSnippet.concat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ajjpj.acollections.AList;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.common.RawRowExtractor;
import com.ajjpj.asqlmapper.core.common.ScalarRowExtractor;
import com.ajjpj.asqlmapper.core.common.SqlRow;
import com.ajjpj.asqlmapper.core.impl.*;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

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
        return create(PrimitiveTypeRegistry.defaults());
    }

    public static SqlEngine create(PrimitiveTypeRegistry primTypes) {
        return new SqlEngine(primTypes, AOption.none(), CanHandleRegistry.empty(), AVector.empty(), AOption.empty());
    }

    private SqlEngine(PrimitiveTypeRegistry primTypes, AOption<String> optDefaultPkName, CanHandleRegistry<RowExtractor> rowExtractorRegistry,
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

    public RowExtractor rowExtractorFor(Class<?> cls) {
        final AOption<RowExtractor> registered = rowExtractorRegistry.handlerFor(cls);
        if (registered.isDefined()) {
            return registered.get();
        }

        if (primTypes.isPrimitiveType(cls)) {
            return new ScalarRowExtractor(cls);
        }

        throw new IllegalArgumentException("no row extractor registered for " + cls + " - pass in a RowExtractor instance explicitly or register it");
    }

    //--------------------------- generic update statements, i.e. statements not returning a result set

    private AUpdate update(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AUpdateImpl(concat(sql, moreSql), primTypes, listeners, defaultConnectionSupplier);
    }

    private AUpdate update(String sql, Object... params) {
        return new AUpdateImpl(SqlSnippet.sql(sql, params), primTypes, listeners, defaultConnectionSupplier);
    }

    public int executeUpdate(SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).execute();
    }
    public int executeUpdate(Connection conn, SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).execute(conn);
    }

    public int executeUpdate(String sql, Object... params) {
        return update(sql, params).execute();
    }
    public int executeUpdate(Connection conn, String sql, Object... params) {
        return update(sql, params).execute(conn);
    }

    public long executeLargeUpdate(SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).executeLarge();
    }
    public long executeLargeUpdate(Connection conn, SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).executeLarge(conn);
    }

    public long executeLargeUpdate(String sql, Object... params) {
        return update(sql, params).executeLarge();
    }
    public long executeLargeUpdate(Connection conn, String sql, Object... params) {
        return update(sql, params).executeLarge(conn);
    }

    //TODO expected '1 row affected'?

    //--------------------------- batch updates

    public int[] executeBatch(String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).execute();
    }
    public int[] executeBatch(Connection conn, String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).execute(conn);
    }

    public long[] executeLargeBatch(String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).executeLarge();
    }
    public long[] executeLargeBatch(Connection conn, String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).executeLarge(conn);
    }

    public int[] executeBatch(List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).execute();
    }
    public int[] executeBatch(Connection conn, List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).execute(conn);
    }
    public long[] executeLargeBatch(List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).executeLarge();
    }
    public long[] executeLargeBatch(Connection conn, List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).executeLarge(conn);
    }

    //--------------------------- insert statements, i.e. statements returning a generated primary key

    private String defaultPkName() {
        return optDefaultPkName
                .orElseThrow(() -> new IllegalStateException("no default PK name was defined - call 'ASqlEngine.withDefaultPkName()' to set it"));
    }

    public UUID insertUuidPk(String sql, Object... params) {
        return insertUuidPk(SqlSnippet.sql(sql, params));
    }
    public String insertStringPk(String sql, Object... params) {
        return insertStringPk(SqlSnippet.sql(sql, params));
    }
    public int insertIntegerPk(String sql, Object... params) {
        return insertIntegerPk(SqlSnippet.sql(sql, params));
    }
    public long insertLongPk(String sql, Object... params) {
        return insertLongPk(SqlSnippet.sql(sql, params));
    }
    public <T> T insertSingleColPk(Class<T> pkType, String sql, Object... params) {
        return insertSingleColPk(pkType, SqlSnippet.sql(sql, params));
    }

    public UUID insertUuidPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertUuidPkInCol(defaultPkName(), sql, moreSql);
    }
    public String insertStringPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertStringPkInCol(defaultPkName(), sql, moreSql);
    }
    public int insertIntegerPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertIntegerPkInCol(defaultPkName(), sql, moreSql);
    }
    public long insertLongPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertLongPkInCol(defaultPkName(), sql, moreSql);
    }
    public <T> T insertSingleColPk(Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insertSingleColPkInCol(defaultPkName(), pkType, sql, moreSql);
    }

    public UUID insertUuidPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(UUID.class, ScalarRowExtractor.UUID_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public String insertStringPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(String.class, ScalarRowExtractor.STRING_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public int insertIntegerPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(Integer.class, ScalarRowExtractor.INT_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    public long insertLongPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }

    public <T> T insertSingleColPkInCol(String colName, Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(pkType, new ScalarRowExtractor(pkType), concat(sql, moreSql), colName);
    }
    public <T> T insertSingleColPkInCol(Connection conn, String colName, Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(conn, pkType, new ScalarRowExtractor(pkType), concat(sql, moreSql), colName);
    }

    public <T> T insert(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, String colName1, String... colNames) {
        return insert(pkType, rowExtractor, sql, AVector.<String>builder().add(colName1).addAll(colNames).build());
    }
    public <T> T insert(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, List<String> colNames) {
        return new AInsertImpl<>(pkType, sql, primTypes, rowExtractor, colNames, listeners, defaultConnectionSupplier).executeSingle();
    }

    public <T> T insert(Connection conn, Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, String colName1, String... colNames) {
        return insert(conn, pkType, rowExtractor, sql, AVector.<String>builder().add(colName1).addAll(colNames).build());
    }
    public <T> T insert(Connection conn, Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, List<String> colNames) {
        return new AInsertImpl<>(pkType, sql, primTypes, rowExtractor, colNames, listeners, defaultConnectionSupplier).executeSingle(conn);
    }

    public <T> AList<T> insertMulti(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, String colName1, String... colNames) {
        return insertMulti(pkType, rowExtractor, sql, AVector.<String>builder().add(colName1).addAll(colNames).build());
    }
    public <T> AList<T> insertMulti(Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, List<String> colNames) {
        return new AInsertImpl<>(pkType, sql, primTypes, rowExtractor, colNames, listeners, defaultConnectionSupplier).executeMulti();
    }

    public <T> AList<T> insertMulti(Connection conn, Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, String colName1, String... colNames) {
        return insertMulti(conn, pkType, rowExtractor, sql, AVector.<String>builder().add(colName1).addAll(colNames).build());
    }
    public <T> AList<T> insertMulti(Connection conn, Class<T> pkType, RowExtractor rowExtractor, SqlSnippet sql, List<String> colNames) {
        return new AInsertImpl<>(pkType, sql, primTypes, rowExtractor, colNames, listeners, defaultConnectionSupplier).executeMulti(conn);
    }

    // -------------------------- select statements

    public <T> AQuery<T> scalarQuery(Class<T> columnType, SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(columnType, concat(sql, moreSql), primTypes, new ScalarRowExtractor(columnType), listeners, defaultConnectionSupplier,
                AVector.empty());
    }
    public <T> AQuery<T> scalarQuery(Class<T> columnType, String sql, Object... params) {
        return scalarQuery(columnType, SqlSnippet.sql(sql, params));
    }

    public AQuery<Long> longQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Long.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.LONG_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty());
    }
    public AQuery<Long> longQuery(String sql, Object... params) {
        return longQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Integer> intQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Integer.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.INT_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty());
    }
    public AQuery<Integer> intQuery(String sql, Object... params) {
        return intQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<String> stringQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(String.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.STRING_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty());
    }
    public AQuery<String> stringQuery(String sql, Object... params) {
        return stringQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<UUID> uuidQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(UUID.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.UUID_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty());
    }
    public AQuery<UUID> uuidQuery(String sql, Object... params) {
        return uuidQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Double> doubleQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Double.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.DOUBLE_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty());
    }
    public AQuery<Double> doubleQuery(String sql, Object... params) {
        return doubleQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<BigDecimal> bigDecimalQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(BigDecimal.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.BIG_DECIMAL_EXTRACTOR, listeners,
                defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<BigDecimal> bigDecimalQuery(String sql, Object... params) {
        return bigDecimalQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Boolean> booleanQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Boolean.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.BOOLEAN_EXTRACTOR, listeners,
                defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<Boolean> booleanQuery(String sql, Object... params) {
        return booleanQuery(SqlSnippet.sql(sql, params));
    }



    public AQuery<SqlRow> rawQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(SqlRow.class, concat(sql, moreSql), primTypes, RawRowExtractor.INSTANCE, listeners, defaultConnectionSupplier, AVector.empty());
    }
    public AQuery<SqlRow> rawQuery(String sql, Object... params) {
        return new AQueryImpl<>(SqlRow.class, SqlSnippet.sql(sql, params), primTypes, RawRowExtractor.INSTANCE, listeners, defaultConnectionSupplier,
                AVector.empty());
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
     * does <em>not</em> do any resource handling, so it should <em>not</em> be used to pull a new connection from
     * a thread pool or data source - that would cause a resource leak because the connection would never be closed.
     */
    public SqlEngine withDefaultConnectionSupplier(Supplier<Connection> supp) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry, listeners, AOption.some(supp));
    }
}

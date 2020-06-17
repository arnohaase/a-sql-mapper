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
import com.ajjpj.asqlmapper.core.impl.AQueryImpl;
import com.ajjpj.asqlmapper.core.impl.CanHandleRegistry;
import com.ajjpj.asqlmapper.core.impl.Constants;
import com.ajjpj.asqlmapper.core.listener.SqlEngineEventListener;

/**
 * SqlEngine provides an API for executing SQL statements. It is a thin wrapper around JDBC, improving primitive type handling and providing a
 *  cleaner and simpler API while maintaining the abstraction of directly executed SQL statements.<p>
 *
 * Specifically, it is no O/R mapper and does not generate SQL code or try to abstract away the underlying database system. Clients need to pass
 *  in SQL statements that are passed to the underlying database 'as is', and connection and transaction handling are completely up to client code.
 *  {@code SqlEngine} is the core of the {@code a-sql-mapper} library, and it is written to be usable on its own. There is a higher level of
 *  abstraction ({@link com.ajjpj.asqlmapper.mapper.SqlMapper SqlMapper}) built on top of {@code SqlEngine} that provides O/R mapper like functionality,
 *  but it is not necessary to use that level of abstraction, and both can be used in combination.<p>
 *
 * SqlEngine instances hold their configuration and allow modifying that configuration. They are immutable, and modifying configuration (calling one
 *  of the 'with...' methods, e.g. {@link #withRawTypeMapping(Class, Function)}) returns a modified copy of the SqlEngine, leaving the original
 *  unmodified and fully usable.<p>
 *
 * The preferred way of configuring and instantiating a SqlEngine instance is to use a {@link com.ajjpj.asqlmapper.SqlMapperBuilder SqlMapperBuilder}.
 *  Calling the {@link #create()} factory method however creates a usable instance that should fully satisfy simple use cases and can be customized by
 *  calling configuration methods:
 *  <ul>
 *      <li> {@link #withDefaultConnectionSupplier(Supplier)} configures a supplier for 'the current' JDBC {@link Connection}, e.g. from a ThreadLocal. Without
 *           a default connection supplier set on the {@code SqlEngine}, clients must passed in a {@link Connection} wherever one is needed (e.g.
 *           {@link #executeUpdate(Connection, String, Object...)}) - if it is configured, clients may (but are not required to) use method variants without
 *           a connection parameter (e.g. {@link #executeUpdate(String, Object...)}.
 *      <li> {@link #withDefaultPkName(String)} sets a default column name for 'auto generated' primary key values. This is used by the {@code insert...} methods
 *           (e.g. {@link #insertLongPk(String, Object...)} as opposed to the {@code insert...InCol} methods (e.g.
 *           {@link #insertLongPkInCol(String, String, Object...)}).
 *      <li> {@link #withRawTypeMapping(Class, Function)} registers a default mapping for primitive values of a given type, i.e. a transformation function
 *           that the SqlMapper applies to values of a given type (e.g. {@link java.sql.Timestamp} to {@link java.time.Instant}.
 *      <li> {@link #withPrimitiveHandler(PrimitiveTypeHandler)} registers a {@link PrimitiveTypeHandler} for a given Java type.
 *      <li> {@link #withListener(SqlEngineEventListener)} registers a {@link SqlEngineEventListener}. This listener is called with details of SQL operations,
 *           allowing detailed logging or collection of statistics. {@link com.ajjpj.asqlmapper.core.listener.LoggingListener LoggingListener} provides
 *           default functionality that handles common cases, but application code can register their own listeners.
 *      <li> {@link #withRowExtractor(RowExtractor)} registers a {@link RowExtractor}, i.e. a transformation from a {@link SqlRow} to a given type. This
 *           allows for 'mapping' functionality. Note however that objects created this way are not implicitly linked to the database - changes to the object
 *           do *not* trigger database updates. {@link RowExtractor}s are one-way (from {@link SqlRow to Java object but not the other way round}, and they
 *           require application-written queries. Both is intentional: {@link SqlEngine} is meant to be provide straight-forward wrapper support for
 *           executing SQL statements without any 'magic' getting in the way. {@link com.ajjpj.asqlmapper.mapper.SqlMapper SqlMapper} provides an optional
 *           abstraction layer on top of {@link SqlEngine}, generating SQL code for common cases and offering more of an O/R feeling.
 *  </ul>
 */
public class SqlEngine {
    private final PrimitiveTypeRegistry primTypes;
    private final AOption<String> optDefaultPkName;
    private final CanHandleRegistry<RowExtractor> rowExtractorRegistry;
    private final AVector<SqlEngineEventListener> listeners;
    private final AOption<Supplier<Connection>> defaultConnectionSupplier;
    private final int defaultFetchSize;

    /**
     * Creates a SqlEngine initialized with default primitive type handlers. This is completely usable for simple cases, but building instances with a
     *  {@link com.ajjpj.asqlmapper.SqlMapperBuilder SqlMapperBuilder} is usually preferable because the builder adds convenience and simplifies
     *  configuration.
     */
    public static SqlEngine create() {
        return create(PrimitiveTypeRegistry.defaults());
    }

    /**
     * Creates a SqlEngine with a given {@link PrimitiveTypeRegistry}. This is for special cases where you need full control over the primitive type
     *  handlers being registered and do not want the defaults.
     */
    public static SqlEngine create(PrimitiveTypeRegistry primTypes) {
        return new SqlEngine(primTypes, AOption.none(), CanHandleRegistry.empty(), AVector.empty(), AOption.empty(), Constants.DEFAULT_FETCH_SIZE);
    }

    private SqlEngine(PrimitiveTypeRegistry primTypes, AOption<String> optDefaultPkName, CanHandleRegistry<RowExtractor> rowExtractorRegistry,
                      AVector<SqlEngineEventListener> listeners, AOption<Supplier<Connection>> defaultConnectionSupplier,
                      int defaultFetchSize) {
        this.primTypes = primTypes;
        this.optDefaultPkName = optDefaultPkName;
        this.rowExtractorRegistry = rowExtractorRegistry;
        this.listeners = listeners;
        this.defaultConnectionSupplier = defaultConnectionSupplier;
        this.defaultFetchSize = defaultFetchSize;
    }

    /**
     * @return this SqlEngine's PrimitiveTypeRegistry
     */
    public PrimitiveTypeRegistry primitiveTypeRegistry() {
        return primTypes;
    }

    /**
     * Returns the {@link RowExtractor} registered for a given type. Primitive types (i.e. types with a registered {@link PrimitiveTypeHandler}) implicitly
     *  have a {@link ScalarRowExtractor} defined for them which extracts the value for a single-column {@link SqlRow}.
     *
     * @return the {@link RowExtractor} registered for a given class.
     * @throws IllegalArgumentException if no {@link RowExtractor} is registered for the class
     */
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

    /**
     * Executes a SQL update, concatenating the {@link SqlSnippet SQL snippets} passed in. Update here means any kind of SQL that does not return
     *  a result set, which can be INSERT or DELETE statements or any kind of DDL statements.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the "number of rows affected". This is well-defined for INSERT, UPDATE or DELETE but can depend on the database system for other statements.
     * @throws IllegalStateException if no default connection supplier is {@link #withDefaultConnectionSupplier(Supplier) registered}
     */
    public int executeUpdate(SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).execute();
    }
    /**
     * Executes a SQL update, concatenating the {@link SqlSnippet SQL snippets} passed in. Update here means any kind of SQL that does not return
     *  a result set, which can be INSERT or DELETE statements or any kind of DDL statements.
     *
     * @return the "number of rows affected". This is well-defined for INSERT, UPDATE or DELETE but can depend on the database system for other statements.
     */
    public int executeUpdate(Connection conn, SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).execute(conn);
    }

    /**
     * Executes a SQL update. Update here means any kind of SQL that does not return
     *  a result set, which can be INSERT or DELETE statements or any kind of DDL statements.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the "number of rows affected". This is well-defined for INSERT, UPDATE or DELETE but can depend on the database system for other statements.
     * @throws IllegalStateException if no default connection supplier is {@link #withDefaultConnectionSupplier(Supplier) registered}
     */
    public int executeUpdate(String sql, Object... params) {
        return update(sql, params).execute();
    }
    /**
     * Executes a SQL update. Update here means any kind of SQL that does not return
     *  a result set, which can be INSERT or DELETE statements or any kind of DDL statements.
     *
     * @return the "number of rows affected". This is well-defined for INSERT, UPDATE or DELETE but can depend on the database system for other statements.
     */
    public int executeUpdate(Connection conn, String sql, Object... params) {
        return update(sql, params).execute(conn);
    }

    /**
     * The same as {@link #executeUpdate(SqlSnippet, SqlSnippet...)} except that this method returns the number of affected rows as a {@code long} value,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long executeLargeUpdate(SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).executeLarge();
    }
    /**
     * The same as {@link #executeUpdate(Connection, SqlSnippet, SqlSnippet...)} except that this method returns the number of affected rows as a {@code long} value,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long executeLargeUpdate(Connection conn, SqlSnippet sql, SqlSnippet... moreSql) {
        return update(sql, moreSql).executeLarge(conn);
    }

    /**
     * The same as {@link #executeUpdate(String, Object...)} except that this method returns the number of affected rows as a {@code long} value,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long executeLargeUpdate(String sql, Object... params) {
        return update(sql, params).executeLarge();
    }
    /**
     * The same as {@link #executeUpdate(Connection, String, Object...)} except that this method returns the number of affected rows as a {@code long} value,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long executeLargeUpdate(Connection conn, String sql, Object... params) {
        return update(sql, params).executeLarge(conn);
    }

    //TODO expected '1 row affected'?

    //--------------------------- batch updates

    /**
     * Executes a 'batch' of SQL statements in a single API call, taking a single (parameterized) SQL statement and a list of parameter lists.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param sql    The SQL statement to execute with different parameter lists
     * @param params The list of parameter lists. Each of the list's entries is bound the the SQL statement's parameters.
     *
     * @return       The number of affected rows for each of the parameter lists. This array's size is the same as the param list's.
     * @throws IllegalStateException if no default connection supplier is {@link #withDefaultConnectionSupplier(Supplier) registered}
     */
    public int[] executeBatch(String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).execute();
    }
    /**
     * Executes a 'batch' of SQL statements in a single API call, taking a single (parameterized) SQL statement and a list of parameter lists.<p>
     *
     * @param sql    The SQL statement to execute with different parameter lists
     * @param params The list of parameter lists. Each of the list's entries is bound the the SQL statement's parameters.
     *
     * @return       The number of affected rows for each of the parameter lists. This array's size is the same as the param list's.
     */
    public int[] executeBatch(Connection conn, String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).execute(conn);
    }

    /**
     * The same as {@link #executeBatch(String, List)} except that this method returns the number of affected rows as {@code long} values,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long[] executeLargeBatch(String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).executeLarge();
    }
    /**
     * The same as {@link #executeBatch(Connection, String, List)} except that this method returns the number of affected rows as {@code long} values,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long[] executeLargeBatch(Connection conn, String sql, List<List<?>> params) {
        return new ABatchUpdate(sql, params, primTypes, listeners, defaultConnectionSupplier).executeLarge(conn);
    }

    /**
     * Executes a 'batch' of SQL statements in a single API call.<p>
     *
     * All {@link SqlSnippet}s passed in as items <em>must</em> have the <em>same</em> SQL string and vary only in the bound parameters. This
     *  is due to API restrictions on {@link java.sql.PreparedStatement}. So This method is really an API variant of
     *  {@link #executeBatch(String, List)} with equivalent functionality, so application code can choose which of the two variants
     *  is easier to use in a given context.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param items the {@link SqlSnippet}s to be executed in a batch, which <em>must</em> have the <em>same</em> SQL string
     * @return the number of rows affected by each of the batch items
     * @throws IllegalStateException if no default connection supplier is {@link #withDefaultConnectionSupplier(Supplier) registered}
     */
    public int[] executeBatch(List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).execute();
    }
    /**
     * Executes a 'batch' of SQL statements in a single API call.<p>
     *
     * All {@link SqlSnippet}s passed in as items <em>must</em> have the <em>same</em> SQL string and vary only in the bound parameters. This
     *  is due to API restrictions on {@link java.sql.PreparedStatement}. So This method is really an API variant of
     *  {@link #executeBatch(String, List)} with equivalent functionality, so application code can choose which of the two variants
     *  is easier to use in a given context.<p>
     *
     * @param items the {@link SqlSnippet}s to be executed in a batch, which <em>must</em> have the <em>same</em> SQL string
     * @return the number of rows affected by each of the batch items
     */
    public int[] executeBatch(Connection conn, List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).execute(conn);
    }
    /**
     * The same as {@link #executeBatch(List)} except that this method returns the number of affected rows as {@code long} values,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long[] executeLargeBatch(List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).executeLarge();
    }
    /**
     * The same as {@link #executeBatch(Connection, List)} except that this method returns the number of affected rows as {@code long} values,
     *  allowing updates affecting more than {@link Integer#MAX_VALUE} rows.
     */
    public long[] executeLargeBatch(Connection conn, List<SqlSnippet> items) {
        return new ABatchUpdate(items, primTypes, listeners, defaultConnectionSupplier).executeLarge(conn);
    }

    //--------------------------- insert statements, i.e. statements returning a generated primary key

    private String defaultPkName() {
        return optDefaultPkName
                .orElseThrow(() -> new IllegalStateException("no default PK name was defined - call 'ASqlEngine.withDefaultPkName()' to set it"));
    }

    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} or {@link #withDefaultPkName(String)
     *         default PK name} is registered
     */
    public int insertIntegerPk(String sql, Object... params) {
        return insertIntegerPk(SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultPkName(String) default PK name} is registered
     */
    public int insertIntegerPk(Connection conn, String sql, Object... params) {
        return insertIntegerPk(conn, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} or {@link #withDefaultPkName(String)
     *         default PK name} is registered
     */
    public int insertIntegerPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertIntegerPkInCol(defaultPkName(), sql, moreSql);
    }
    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultPkName(String) default PK name} is registered
     */
    public int insertIntegerPk(Connection conn, SqlSnippet sql, SqlSnippet... moreSql) {
        return insertIntegerPkInCol(conn, defaultPkName(), sql, moreSql);
    }

    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
    public int insertIntegerPkInCol(String colName, String sql, Object... params) {
        return insertIntegerPkInCol(colName, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     */
    public int insertIntegerPkInCol(Connection conn, String colName, String sql, Object... params) {
        return insertIntegerPkInCol(conn, colName, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
    public int insertIntegerPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(Integer.class, ScalarRowExtractor.INT_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    /**
     * Inserts a row with an integer-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * @return the auto-generated primary key
     */
    public int insertIntegerPkInCol(Connection conn, String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(conn, Integer.class, ScalarRowExtractor.INT_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }

    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} or {@link #withDefaultPkName(String)
     *         default PK name} is registered
     */
    public long insertLongPk(String sql, Object... params) {
        return insertLongPk(SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultPkName(String) default PK name} is registered
     */
    public long insertLongPk(Connection conn, String sql, Object... params) {
        return insertLongPk(conn, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} or {@link #withDefaultPkName(String)
     *         default PK name} is registered
     */
    public long insertLongPk(SqlSnippet sql, SqlSnippet... moreSql) {
        return insertLongPkInCol(defaultPkName(), sql, moreSql);
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultPkName(String) default PK name} is registered
     */
    public long insertLongPk(Connection conn, SqlSnippet sql, SqlSnippet... moreSql) {
        return insertLongPkInCol(conn, defaultPkName(), sql, moreSql);
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
    public long insertLongPkInCol(String colName, String sql, Object... params) {
        return insertLongPkInCol(colName, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.
     *
     * @param sql    the INSERT statement
     * @param params the statement's parameters
     * @return       the auto-generated primary key
     */
    public long insertLongPkInCol(Connection conn, String colName, String sql, Object... params) {
        return insertLongPkInCol(conn, colName, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
    public long insertLongPkInCol(String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(Long.class, ScalarRowExtractor.LONG_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }
    /**
     * Inserts a row with a long-valued auto-generated primary key column, returning the auto-generated primary key.
     *
     * @return the auto-generated primary key
     */
    public long insertLongPkInCol(Connection conn, String colName, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(conn, Long.class, ScalarRowExtractor.LONG_EXTRACTOR, concat(sql, moreSql), AVector.of(colName));
    }

    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} or {@link #withDefaultPkName(String)
     *         default PK name} is registered
     */
    public <T> T insertSingleColPk(Class<T> pkType, String sql, Object... params) {
        return insertSingleColPk(pkType, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.<p>
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultPkName(String) default PK name} is registered
     */
    public <T> T insertSingleColPk(Connection conn, Class<T> pkType, String sql, Object... params) {
        return insertSingleColPk(conn, pkType, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} or {@link #withDefaultPkName(String)
     *         default PK name} is registered
     */
    public <T> T insertSingleColPk(Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insertSingleColPkInCol(defaultPkName(), pkType, sql, moreSql);
    }
    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the configured {@link #withDefaultPkName(String) default PK name} as the auto-generated column's name.<p>
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     * @throws IllegalStateException if no  {@link #withDefaultPkName(String) default PK name} is registered
     */
    public <T> T insertSingleColPk(Connection conn, Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insertSingleColPkInCol(conn, defaultPkName(), pkType, sql, moreSql);
    }

    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
    public <T> T insertSingleColPkInCol(String colName, Class<T> pkType, String sql, Object... params) {
        return insertSingleColPkInCol(colName, pkType, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     */
    public <T> T insertSingleColPkInCol(Connection conn, String colName, Class<T> pkType, String sql, Object... params) {
        return insertSingleColPkInCol(conn, colName, pkType, SqlSnippet.sql(sql, params));
    }
    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
    public <T> T insertSingleColPkInCol(String colName, Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(pkType, new ScalarRowExtractor(pkType), concat(sql, moreSql), AVector.of(colName));
    }
    /**
     * Inserts a row with an auto-generated primary key column, returning the auto-generated primary key.
     *
     * @param pkType the primary key's type, used for {@link PrimitiveTypeRegistry#fromSql(Class, Object) raw type mapping} of the returned value
     * @return the auto-generated primary key
     */
    public <T> T insertSingleColPkInCol(Connection conn, String colName, Class<T> pkType, SqlSnippet sql, SqlSnippet... moreSql) {
        return insert(conn, pkType, new ScalarRowExtractor(pkType), concat(sql, moreSql), AVector.of(colName));
    }

    /**
     * Inserts a row, returning the values of a number of columns. This is the most generic (and powerful) variant of insert method in {@link SqlEngine}'s
     *  API, allowing callers to specify more than one column to be returned and applying a caller-provided {@link RowExtractor} to the resulting tuples.
     *  Some databases (e.g. PostgreSQL) support returning non-generated column values (using
     *  {@link java.sql.Connection#prepareStatement(String, String[]) this API}), but that is not required by JDBC, and support may vary from vender to
     *  vendor and between database or driver versions.<p>
     *
     * You probably don't need this method - if you don't understand the above explanation, there is no need to worry. For the vast majority of applications
     *  the single-column {@code insert} methods are completely sufficient.<p>
     *
     * This method uses the connection provided by the default connection supplier, which must be {@link #withDefaultConnectionSupplier(Supplier) registered}.
     *
     * @return the auto-generated primary key
     * @throws IllegalStateException if no {@link #withDefaultConnectionSupplier(Supplier) default connection supplier} is registered
     */
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
                AVector.empty(), defaultFetchSize);
    }
    public <T> AQuery<T> scalarQuery(Class<T> columnType, String sql, Object... params) {
        return scalarQuery(columnType, SqlSnippet.sql(sql, params));
    }

    public AQuery<Long> longQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Long.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.LONG_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty(), defaultFetchSize);
    }
    public AQuery<Long> longQuery(String sql, Object... params) {
        return longQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Integer> intQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Integer.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.INT_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty(), defaultFetchSize);
    }
    public AQuery<Integer> intQuery(String sql, Object... params) {
        return intQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<String> stringQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(String.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.STRING_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty(), defaultFetchSize);
    }
    public AQuery<String> stringQuery(String sql, Object... params) {
        return stringQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<UUID> uuidQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(UUID.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.UUID_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty(), defaultFetchSize);
    }
    public AQuery<UUID> uuidQuery(String sql, Object... params) {
        return uuidQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Double> doubleQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Double.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.DOUBLE_EXTRACTOR, listeners, defaultConnectionSupplier,
                AVector.empty(), defaultFetchSize);
    }
    public AQuery<Double> doubleQuery(String sql, Object... params) {
        return doubleQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<BigDecimal> bigDecimalQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(BigDecimal.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.BIG_DECIMAL_EXTRACTOR, listeners,
                defaultConnectionSupplier, AVector.empty(), defaultFetchSize);
    }
    public AQuery<BigDecimal> bigDecimalQuery(String sql, Object... params) {
        return bigDecimalQuery(SqlSnippet.sql(sql, params));
    }
    public AQuery<Boolean> booleanQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(Boolean.class, concat(sql, moreSql), primTypes, ScalarRowExtractor.BOOLEAN_EXTRACTOR, listeners,
                defaultConnectionSupplier, AVector.empty(), defaultFetchSize);
    }
    public AQuery<Boolean> booleanQuery(String sql, Object... params) {
        return booleanQuery(SqlSnippet.sql(sql, params));
    }



    public AQuery<SqlRow> rawQuery(SqlSnippet sql, SqlSnippet... moreSql) {
        return new AQueryImpl<>(SqlRow.class, concat(sql, moreSql), primTypes, RawRowExtractor.INSTANCE, listeners, defaultConnectionSupplier, AVector.empty(),
                defaultFetchSize);
    }
    public AQuery<SqlRow> rawQuery(String sql, Object... params) {
        return new AQueryImpl<>(SqlRow.class, SqlSnippet.sql(sql, params), primTypes, RawRowExtractor.INSTANCE, listeners, defaultConnectionSupplier,
                AVector.empty(), defaultFetchSize);
    }

    public <T> AQuery<T> query(Class<T> targetType, SqlSnippet sql, SqlSnippet... moreSql) {
        return query(targetType, rowExtractorFor(targetType), concat(sql, moreSql));
    }

    public <T> AQuery<T> query(Class<T> targetType, String sql, Object... params) {
        return query(targetType, SqlSnippet.sql(sql, params));
    }

    public <T> AQuery<T> query(Class<T> cls, RowExtractor rowExtractor, SqlSnippet sql, SqlSnippet... moreSql) { //TODO consistent ordering of parameters
        return new AQueryImpl<>(cls, concat(sql, moreSql), primTypes, rowExtractor, listeners, defaultConnectionSupplier, AVector.empty(), defaultFetchSize);
    }

    //TODO tuples as query results

    //--------------------------- configuration

    public int defaultFetchSize() {
        return defaultFetchSize;
    }

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
        return new SqlEngine(primTypes.withRawTypeMapping(jdbcType, rawMapping), optDefaultPkName, rowExtractorRegistry, listeners, defaultConnectionSupplier,
                defaultFetchSize);
    }
    public SqlEngine withPrimitiveHandler(PrimitiveTypeHandler handler) {
        return new SqlEngine(primTypes.withHandler(handler), optDefaultPkName, rowExtractorRegistry, listeners, defaultConnectionSupplier, defaultFetchSize);
    }

    public SqlEngine withDefaultPkName(String pkName) {
        return new SqlEngine(primTypes, AOption.of(pkName), rowExtractorRegistry, listeners, defaultConnectionSupplier, defaultFetchSize);
    }

    public SqlEngine withRowExtractor(RowExtractor rowExtractor) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry.withHandler(rowExtractor), listeners, defaultConnectionSupplier,
                defaultFetchSize);
    }

    public SqlEngine withListener(SqlEngineEventListener listener) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry, listeners.append(listener), defaultConnectionSupplier, defaultFetchSize);
    }

    public SqlEngine withDefaultFetchSize(int defaultFetchSize) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry, listeners, defaultConnectionSupplier, defaultFetchSize);
    }

    /**
     * This is for using a connection from e.g. a ThreadLocal rather than requiring it to be passed in explicitly. It
     * does <em>not</em> do any resource handling, so it should <em>not</em> be used to pull a new connection from
     * a thread pool or data source - that would cause a resource leak because the connection would never be closed.
     */
    public SqlEngine withDefaultConnectionSupplier(Supplier<Connection> supp) {
        return new SqlEngine(primTypes, optDefaultPkName, rowExtractorRegistry, listeners, AOption.some(supp), defaultFetchSize);
    }
}

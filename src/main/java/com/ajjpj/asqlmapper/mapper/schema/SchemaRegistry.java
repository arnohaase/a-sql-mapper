package com.ajjpj.asqlmapper.mapper.schema;

import static com.ajjpj.acollections.util.AUnchecker.*;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.impl.SqlHelper;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;

public class SchemaRegistry {
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistry.class);

    private final DatabaseDialect dialect;

    private final Map<String, AOption<TableMetaData>> tableCache = new ConcurrentHashMap<>();

    public SchemaRegistry (DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    public void clearCache() {
        tableCache.clear();
    }

    private JDBCType typeFor(int tpe) {
        try {
            return JDBCType.valueOf(tpe);
        }
        catch(Exception exc) {
            return JDBCType.OTHER;
        }
    }
    private AOption<Class<?>> classFor(String fqn) {
        try {
            return AOption.of(Class.forName(fqn));
        }
        catch (Exception e) {
            return AOption.empty();
        }
    }

    public TableMetaData getRequiredTableMetaData(Connection conn, String tableName) {
        return getTableMetaData(conn, tableName)
                .orElseThrow(() ->
                        executeUnchecked(() -> new IllegalArgumentException("table " + tableName + " does not exist (in schema " + conn.getSchema() + ")")));
    }

    public AOption<TableMetaData> getTableMetaData(Connection conn, String tableName) {
        return tableCache.computeIfAbsent(tableName.toLowerCase(), n -> executeUnchecked (() -> {
            try {
                final PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
                try {
                    final AVector.Builder<ColumnMetaData> columns = AVector.builder();
                    final ResultSetMetaData rsMeta = ps.executeQuery().getMetaData();

                    final ASet<String> pkColumnNames;
                    try (ResultSet rs = conn.getMetaData().getPrimaryKeys(dialect.normalizeCatalogName(conn.getCatalog()),
                            dialect.normalizeSchemaName(conn.getSchema()), dialect.normalizeTableName(tableName))) {
                        pkColumnNames = pkColumnNames(rs);
                    }

                    final AVector<ForeignKeySpec> foreignKeys;
                    try (ResultSet rs = conn.getMetaData().getImportedKeys(dialect.normalizeCatalogName(conn.getCatalog()),
                            dialect.normalizeSchemaName(conn.getSchema()), dialect.normalizeTableName(tableName))) {
                        foreignKeys = fks(rs);
                    }

                    for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                        final String colName = rsMeta.getColumnName(i);
                        final AOption<Class<?>> colClass = classFor(rsMeta.getColumnClassName(i));
                        final JDBCType colType = typeFor(rsMeta.getColumnType(i));
                        final String colTypeName = rsMeta.getColumnTypeName(i);
                        final int size = rsMeta.getColumnDisplaySize(i);
                        final int precision = rsMeta.getPrecision(i);
                        final int scale = rsMeta.getScale(i);

                        final boolean isAutoIncrement = rsMeta.isAutoIncrement(i);
                        final boolean isNullable = rsMeta.isNullable(i) == ResultSetMetaData.columnNullable;

                        columns.add(new ColumnMetaData(colName, colClass, colType, colTypeName, size, precision, scale, pkColumnNames.contains(colName.toUpperCase()), isAutoIncrement, isNullable));
                    }
                    return AOption.of(new TableMetaData(tableName, columns.build(), foreignKeys));
                }
                finally {
                    SqlHelper.closeQuietly(ps);
                }
            }
            catch(SQLException exc) {
                log.debug("could not retrieve meta data for table " + tableName + " - this may be because a bean has no corresponding 'default' table, which is perfectly fine for queries but prevents it from being used for mapped write access", exc);
                return AOption.empty();
            }
        }));
    }

    private AVector<ForeignKeySpec> fks(ResultSet rs) throws SQLException {
        AMap<String,ForeignKeySpec> result = AMap.empty();
        while(rs.next()) {
            final String fkName = rs.getString("FK_NAME");

            final String fkColName = rs.getString("FKCOLUMN_NAME");
            final String fkTableName = rs.getString("FKTABLE_NAME");
            final String pkColName = rs.getString("PKCOLUMN_NAME");
            final String pkTableName = rs.getString("PKTABLE_NAME");

            if(result.containsKey(fkName)) {
                result = result.minus(fkName);
                log.warn("multi-column foreign key " + fkName + " in table " + fkTableName + " referencing table " + pkTableName + " -> ignored");
                continue;
            }

            result = result.plus(fkName, new ForeignKeySpec(fkColName, fkTableName, pkColName, pkTableName));
        }

        return result.values().toVector();
    }

    private ASet<String> pkColumnNames(ResultSet rs) throws SQLException {
        ASet<String> result = ASet.empty();
        while (rs.next()) {
            result = result.plus(rs.getString("COLUMN_NAME").toUpperCase());
        }
        return result;
    }
}


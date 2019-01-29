package com.ajjpj.asqlmapper.mapper.schema;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.acollections.util.AOption;
import com.ajjpj.asqlmapper.core.impl.SqlHelper;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

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

    public AOption<TableMetaData> getTableMetaData(Connection conn, String tableName) {
        return tableCache.computeIfAbsent(tableName.toLowerCase(), n -> executeUnchecked (() -> {
            try {
                final PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
                try {
                    final AVector.Builder<ColumnMetaData> columns = AVector.builder();
                    final ResultSetMetaData rsMeta = ps.executeQuery().getMetaData();

                    final ASet<String> pkColumnNames;
                    try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, dialect.normalizeSchemaName(conn.getSchema()), dialect.normalizeTableName(tableName))) {
                        pkColumnNames = pkColumnNames(rs);
                    }

//TODO            conn.getMetaData().getImportedKeys()

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
                    return AOption.of(new TableMetaData(tableName, columns.build()));
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

    private final ASet<String> pkColumnNames(ResultSet rs) throws SQLException {
        ASet<String> result = ASet.empty();
        while (rs.next()) result = result.plus(rs.getString("COLUMN_NAME").toUpperCase());
        return result;
    }
}


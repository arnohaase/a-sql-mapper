package com.ajjpj.asqlmapper.mapper.schema;

import com.ajjpj.acollections.ASet;
import com.ajjpj.acollections.immutable.AVector;
import com.ajjpj.asqlmapper.core.impl.SqlHelper;
import com.ajjpj.asqlmapper.mapper.DatabaseDialect;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ajjpj.acollections.util.AUnchecker.executeUnchecked;

public class SchemaRegistry {
    private final DatabaseDialect dialect;

    private final Map<String, TableMetaData> tableCache = new ConcurrentHashMap<>();

    public SchemaRegistry (DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    public void clearCache() {
        tableCache.clear();
    }

    public TableMetaData getTableMetaData(Connection conn, String tableName) throws SQLException, ClassNotFoundException {
        return tableCache.computeIfAbsent(tableName.toLowerCase(), n -> executeUnchecked (() -> {
            final ASet<String> pkColumnNames;
            try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, dialect.normalizeSchemaName(conn.getSchema()), dialect.normalizeTableName(tableName))) {
                pkColumnNames = pkColumnNames(rs);
            }

//TODO            conn.getMetaData().getImportedKeys()

            final PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
            try {
                final AVector.Builder<ColumnMetaData> columns = AVector.builder();

                final ResultSetMetaData rsMeta = ps.executeQuery().getMetaData();
                for (int i=1; i<=rsMeta.getColumnCount(); i++) {
                    final String colName = rsMeta.getColumnName(i);
                    final Class<?> colClass = Class.forName(rsMeta.getColumnClassName(i)); //TODO secure this
                    final JDBCType colType = JDBCType.valueOf(rsMeta.getColumnType(i));
                    final String colTypeName = rsMeta.getColumnTypeName(i);
                    final int size = rsMeta.getColumnDisplaySize(i);
                    final int precision = rsMeta.getPrecision(i);
                    final int scale = rsMeta.getScale(i);

                    final boolean isAutoIncrement = rsMeta.isAutoIncrement(i);
                    final boolean isNullable = rsMeta.isNullable(i) == ResultSetMetaData.columnNullable;

                    columns.add(new ColumnMetaData(colName, colClass, colType, colTypeName, size, precision, scale, pkColumnNames.contains(colName.toUpperCase()), isAutoIncrement, isNullable));
                }
                return new TableMetaData(tableName, columns.build());
            }
            finally {
                SqlHelper.closeQuietly(ps);
            }
        }));
    }

    private final ASet<String> pkColumnNames(ResultSet rs) throws SQLException {
        ASet<String> result = ASet.empty();
        while (rs.next()) result = result.plus(rs.getString("COLUMN_NAME").toUpperCase());
        return result;
    }
}


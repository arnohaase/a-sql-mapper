package com.ajjpj.asqlmapper.mapper;


public interface DatabaseDialect {
    DatabaseDialect H2 = new H2Dialect();
    DatabaseDialect POSTGRESQL = new PostgresqlDialect();
    DatabaseDialect ORACLE = new OracleDialect();
    DatabaseDialect SQL_SERVER = new SqlServerDialect();

    default String normalizeCatalogName(String catalogName) {
        return catalogName;
    }
    default String normalizeSchemaName(String schemaName) {
        return schemaName;
    }
    default String normalizeTableName(String tableName) {
        return tableName;
    }

    /**
     * This returns a SELECT statement that returns a single row from a given table, it is used to limit the
     *  amount of prefetched data when retrieving table metadata.
     */
    String selectOneRow(String tableName);

    class PostgresqlDialect implements DatabaseDialect {
        @Override public String selectOneRow(String tableName) {
            return "SELECT * FROM " + tableName + " LIMIT 1";
        }
    }

    class OracleDialect implements DatabaseDialect {
        @Override public String selectOneRow(String tableName) {
            return "SELECT * FROM " + tableName + " FETCH FIRST 1 ROWS ONLY";
        }
    }

    class SqlServerDialect implements DatabaseDialect {
        @Override public String selectOneRow(String tableName) {
            return "SELECT TOP 1 * FROM " + tableName;
        }
    }

    class H2Dialect implements DatabaseDialect {
        @Override public String normalizeCatalogName (String catalogName) {
            return catalogName.toUpperCase();
        }
        @Override public String normalizeSchemaName (String schemaName) {
            return schemaName.toUpperCase();
        }
        @Override public String normalizeTableName (String schemaName) {
            return schemaName.toUpperCase();
        }

        @Override public String selectOneRow(String tableName) {
            return "SELECT * FROM " + tableName + " LIMIT 1";
        }
    }
}

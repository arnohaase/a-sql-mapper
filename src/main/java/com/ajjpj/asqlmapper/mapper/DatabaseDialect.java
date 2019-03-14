package com.ajjpj.asqlmapper.mapper;


public interface DatabaseDialect {
    DatabaseDialect H2 = new H2Dialect();
    DatabaseDialect POSTGRESQL = new PostgresqlDialect();
    DatabaseDialect ORACLE = new OracleDialect();
    DatabaseDialect SQL_SERVER = new SqlServerDialect();

    default String normalizeSchemaName(String schemaName) {
        return schemaName;
    }
    default String normalizeTableName(String tableName) {
        return tableName;
    }

    class PostgresqlDialect implements DatabaseDialect {
    }

    class OracleDialect implements DatabaseDialect {
    }

    class SqlServerDialect implements DatabaseDialect {
    }

    class H2Dialect implements DatabaseDialect {
        @Override public String normalizeSchemaName (String schemaName) {
            return schemaName.toUpperCase();
        }
        @Override public String normalizeTableName (String schemaName) {
            return schemaName.toUpperCase();
        }
    }
}

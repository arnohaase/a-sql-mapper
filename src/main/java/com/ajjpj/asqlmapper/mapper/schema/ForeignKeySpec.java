package com.ajjpj.asqlmapper.mapper.schema;


public class ForeignKeySpec {
    private final String fkTableName;
    private final String fkColumnName;
    private final String pkColumnName;
    private final String pkTableName;

    public ForeignKeySpec (String fkColumnName, String fkTableName, String pkColumnName, String pkTableName) {
        this.fkColumnName = fkColumnName;
        this.fkTableName = fkTableName;
        this.pkColumnName = pkColumnName;
        this.pkTableName = pkTableName;
    }

    public String fkColumnName() {
        return fkColumnName;
    }

    public String fkTableName() {
        return fkTableName;
    }

    public String pkColumnName() {
        return pkColumnName;
    }

    public String pkTableName() {
        return pkTableName;
    }

    @Override public String toString() {
        return "ForeignKeySpec{" +
                "fkColumnName='" + fkColumnName + '\'' +
                ", fkTableName='" + fkTableName + '\'' +
                ", pkColumnName='" + pkColumnName + '\'' +
                ", pkTableName='" + pkTableName + '\'' +
                '}';
    }
}

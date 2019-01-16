package com.ajjpj.asqlmapper;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;


public abstract class AbstractDatabaseTest {
    protected static final DataSource ds = JdbcConnectionPool.create("jdbc:h2:mem:unittest;MVCC=TRUE;MODE=PostgreSQL", "sa", "");
    protected Connection conn;

    @BeforeEach void openConnection() throws SQLException {
        conn = ds.getConnection();
        conn.setAutoCommit(false);
    }

    @AfterEach void closeConnection() throws SQLException {
        conn.rollback();
        conn.close();
    }
}

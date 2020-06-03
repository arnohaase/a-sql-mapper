package com.ajjpj.asqlmapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

import com.ajjpj.asqlmapper.testutil.ResourceBookkeepingDataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractDatabaseTest {

    private static final DataSource innerDs = JdbcConnectionPool.create("jdbc:h2:mem:unittest;MVCC=TRUE;MODE=PostgreSQL", "sa", "");

    protected final DataSource ds = new ResourceBookkeepingDataSource(innerDs);
    protected Connection conn;

    @BeforeEach
    void openConnection() throws SQLException {
        conn = ds.getConnection();
        conn.setAutoCommit(false);
    }

    @AfterEach
    void closeConnection() throws SQLException {
        conn.rollback();
        conn.close();
        assertResourcesReleased();
    }

    protected void executeUpdate(String sql) throws SQLException {
        try (PreparedStatement stmnt = conn.prepareStatement(sql)) {
            stmnt.executeUpdate();
        }
    }

    private void assertResourcesReleased() {
        ((ResourceBookkeepingDataSource) ds).assertResourcesReleased();
    }
}

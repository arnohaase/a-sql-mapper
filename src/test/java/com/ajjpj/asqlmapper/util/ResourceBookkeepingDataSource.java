package com.ajjpj.asqlmapper.util;

import static com.ajjpj.acollections.util.AUnchecker.*;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

import com.ajjpj.acollections.AMap;
import com.ajjpj.acollections.mutable.AMutableMapWrapper;

public class ResourceBookkeepingDataSource implements DataSource {
    private final DataSource delegate;

    private AMap<ResourceBookkeepingConnection, Throwable> openedConnections = AMutableMapWrapper.empty();

    public ResourceBookkeepingDataSource(DataSource delegate) {
        this.delegate = delegate;
    }

    public void assertResourcesReleased() {
        openedConnections
                .filter(e -> !executeUnchecked(() -> e.getKey().isClosed()))
                .forEach(e -> e.getValue().printStackTrace());

        if (openedConnections
                .keySet()
                .exists(c -> !executeUnchecked(c::isClosed))) {
            fail("unclosed connections");
        }

        openedConnections
                .keySet()
                .forEach(ResourceBookkeepingConnection::assertResourcesReleased);
    }

    private Connection registerConnection(Connection connection) {
        ResourceBookkeepingConnection conn = new ResourceBookkeepingConnection(connection);
        openedConnections.put(conn, new Throwable("stack trace for Connection creation"));
        return conn;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = delegate.getConnection();
        return registerConnection(connection);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Connection connection = delegate.getConnection(username, password);
        return registerConnection(connection);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(ResourceBookkeepingDataSource.class)) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isAssignableFrom(ResourceBookkeepingDataSource.class)) {
            return true;
        }
        return delegate.isWrapperFor(iface);
    }
}

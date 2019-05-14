package com.ajjpj.asqlmapper.core;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Represents a non-SELECT SQL statement, i.e. a statement that does not return a ResultSet
 */
public interface AUpdate {
    int execute(Connection conn);
    int execute();
    long executeLarge(Connection conn);
    long executeLarge();
}

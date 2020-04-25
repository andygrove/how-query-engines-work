package org.ballistacompute.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * JDBC Driver unit tests.
 */
public class DriverTest {

  final Driver driver = new Driver();

  @Test
  public void acceptsValidUrl() throws SQLException {
    assertTrue(driver.acceptsURL("jdbc:arrow://localhost:50051"));
  }

  @Test
  public void rejectsInvalidUrl() throws SQLException {
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost:50051"));
  }

  @Test
  public void rejectsNullUrl() throws SQLException {
    assertFalse(driver.acceptsURL(null));
  }

  /**
   * Note that this is a manual integration test that requires the Rust flight-server example to be running.
   */
  @Test
  @Ignore
  public void executeQuery() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:arrow://localhost:50051", new Properties())) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT id FROM alltypes_plain")) {

          ResultSetMetaData md = rs.getMetaData();
          assertEquals(1, md.getColumnCount());
          assertEquals("c0", md.getColumnName(1));
          assertEquals(Types.INTEGER, md.getColumnType(1));

          List<Integer> ids = new ArrayList<>();
          while (rs.next()) {
            ids.add(rs.getInt(1));
          }
          assertEquals(ImmutableList.of(4, 5, 6, 7, 2, 3, 0, 1), ids);
        }
      }
    }
  }
}

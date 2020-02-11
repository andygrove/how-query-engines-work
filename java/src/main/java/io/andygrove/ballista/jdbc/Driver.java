package io.andygrove.ballista.jdbc;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;

/**
 * Driver.
 */
public class Driver implements java.sql.Driver {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Driver.class);

  /** JDBC connection string prefix. */
  private static final String PREFIX = "jdbc:arrow://";

  @Override
  public Connection connect(String url, Properties properties) throws SQLException {
    logger.info("connect() url={}", url);
    //TODO this needs much more work to parse full URLs but this is enough to get end to end tests running
    String c = url.substring(PREFIX.length());
    int i = c.indexOf(':');
    if (i == -1) {
      return new FlightConnection(c, 50051);
    } else {
      return new FlightConnection(c.substring(0,i), Integer.parseInt(c.substring(i + 1)));
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url != null && url.startsWith(PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 16;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
}

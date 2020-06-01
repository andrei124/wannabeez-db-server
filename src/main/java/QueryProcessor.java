import org.postgis.PGgeometry;

import java.io.*;
import java.sql.*;
import java.util.*;

public class QueryProcessor {

  private static final String CONFIG_FILEPATH =
      System.getenv("DB_CONFIG") != null ? System.getenv("DB_CONFIG") : "src/config.properties";

  private Properties properties;
  private Connection connection;

  public QueryProcessor(Connection connection) {
    this.connection = connection;

    File configFile = new File(CONFIG_FILEPATH);
    properties = new Properties();

    try {
      FileReader fileReader = new FileReader(configFile);
      properties.load(fileReader);
      String driver = properties.getProperty("driver");
      registerDriver(driver);
    } catch (IOException e) {
      System.out.println("Exception occured when loading the config file");
      e.printStackTrace();
    }
  }

  public QueryProcessor() {
    /* Do not connect by default upon creation */
    this(null);
  }

  /**
   * Method for registering a JDBC Driver for PostgreSQL *
   *
   * @param driver -- driver name
   */
  private void registerDriver(String driver) {
    try {
      Class.forName(driver);
      System.out.println("Database driver loaded succesfully");
    } catch (ClassNotFoundException e) {
      System.out.println("JDBC Driver could not be registered");
      System.exit(1);
      e.printStackTrace();
    }
  }

  /**
   * Method for establishing a connection with the DB Server *
   *
   * @return Connection object
   */
  public Connection connect() {
    String url = properties.getProperty("url");
    String user = properties.getProperty("user");
    String password = properties.getProperty("password");

    try {
      connection = DriverManager.getConnection(url, user, password);
      System.out.println("Connection to PostgreSQL server established successfully\n");
    } catch (SQLException e) {
      System.out.println("SQL Exception: " + e.getMessage());
    }
    return connection;
  }

  /** Method for closing an existing connection with the DB Server * Connection must not be null */
  public void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
        System.out.println("Connection succesfully closed");
      } catch (SQLException e) {
        System.out.println("Error occured when closing connection");
        e.printStackTrace();
      }
    }
  }

  /**
   * Methods for Prepared INSERT SQL Statements * Overloaded methods to support all possible types
   * of values to be inserted into DB Each overloaded method is designed for a specific table
   *
   * <p>Insert SQL Prepared statement for PLAYER *
   */
  public void insert(String tableName, Integer id, String email, String password)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?, ?)");
    stmt.setInt(1, id);
    stmt.setString(2, email);
    stmt.setString(3, password);
    executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for PLAYER_STATS * */
  public void insert(String tableName, Integer id, Integer xp, Integer cash) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?, ?)");
    stmt.setInt(1, id);
    stmt.setInt(2, xp);
    stmt.setInt(3, cash);
    executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for GALLERY * */
  public void insert(String tableName, Integer id, Timestamp ts, Integer playerId, String url)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?, ?, ?)");
    stmt.setInt(1, id);
    stmt.setTimestamp(2, ts);
    stmt.setInt(3, playerId);
    stmt.setString(4, url);
    executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for LOCATION * */
  public void insert(String tableName, Integer id, PGgeometry location) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?)");
    stmt.setInt(1, id);
    stmt.setObject(2, location);
    executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for LANDMARK * */
  public void insert(
      String tableName, Integer id, PGgeometry location, Integer type, String description)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?, ?, ?)");
    stmt.setInt(1, id);
    stmt.setObject(2, location);
    stmt.setInt(3, type);
    stmt.setString(4, description);
    executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for LANDMARK_TYPE * */
  public void insert(String tableName, Integer id, String name) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?)");
    stmt.setInt(1, id);
    stmt.setString(2, name);
    executeSQLStatement(stmt);
  }

  private void executeSQLStatement(PreparedStatement stmt) throws SQLException {
    stmt.executeUpdate();
  }

  /**
   * Method for Prepared SELECT SQL statement *
   *
   * @param tableName -- name of table to make query on
   * @param columns -- columns to be selected
   * @return ResultSet -- object representing retrieved rows
   * @throws SQLException
   */
  public ResultSet selectFrom(String tableName, String... columns) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "SELECT " + getColumnsToBeQueried(columns) + " FROM " + tableName);

    String queryColums = getColumnsToBeQueried(columns);
    System.out.println("Columns to be queired: " + queryColums);

    return getResultSet(stmt);
  }

  /**
   * Method for Prepared SELECT with WHERE clause SQL statements * Method is overloaded for each of
   * the different types an entry in the database can take
   *
   * @param tableName -- name of table to make query on
   * @param indexColumn -- filtering parameter in the WHERE clause
   * @param value -- value required for the filtering parameter
   * @param arguments -- columns to be selected
   * @return ResultSet -- object representing retrieved rows
   * @throws SQLException
   */
  public ResultSet selectFromWhere(
      String tableName, String indexColumn, String value, String... arguments) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "SELECT "
                + getColumnsToBeQueried(arguments)
                + " FROM "
                + tableName
                + " WHERE "
                + indexColumn
                + " = ?");

    stmt.setString(4, value);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, Integer value, String... arguments)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "SELECT "
                + getColumnsToBeQueried(arguments)
                + " FROM "
                + tableName
                + " WHERE "
                + indexColumn
                + " = ?");

    stmt.setInt(4, value);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, PGgeometry value, String... arguments)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "SELECT "
                + getColumnsToBeQueried(arguments)
                + " FROM "
                + tableName
                + " WHERE "
                + indexColumn
                + " = ?");

    stmt.setObject(4, value);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, Timestamp value, String... arguments)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "SELECT "
                + getColumnsToBeQueried(arguments)
                + " FROM "
                + tableName
                + " WHERE "
                + indexColumn
                + " = ?");

    stmt.setTimestamp(4, value);

    return getResultSet(stmt);
  }

  /**
   * Helper method for getting the columns to be queried into the SELECT statement *
   *
   * @param arguments - list of column names to be selected
   * @return
   */
  private String getColumnsToBeQueried(String... arguments) {
    List<String> args = Arrays.asList(arguments);
    int len = args.size();
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < len; i++) {
      if (i != len - 1) {
        sb.append(args.get(i)).append(",");
      } else {
        sb.append(args.get(i));
      }
    }
    return sb.toString();
  }

  /**
   * Helper method for retrieving the result of an SQL query *
   *
   * @param stmt - statement to retrieve the result for
   * @return
   * @throws SQLException
   */
  private ResultSet getResultSet(PreparedStatement stmt) throws SQLException {
    return stmt.executeQuery();
  }

  /**
   * Method for Prepared UPDATE SQL statement * Method is overloaded to support UPDATE statements
   * for every possible combination of (setValue, whereValue) (e.g. UPDATE ? SET ? = String WHERE ?
   * = Integer)
   *
   * @param table -- name of table to be updated
   * @param setParam -- name of column to be set
   * @param setValue -- updated value of the column to be set
   * @param whereParam -- name of filtering parameter in WHERE clause
   * @param whereValue -- actual value of filtering parameter
   * @throws SQLException
   */
  public void update(
      String table, String setParam, String setValue, String whereParam, String whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setString(1, setValue);
    stmt.setString(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, String setValue, String whereParam, Integer whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setString(1, setValue);
    stmt.setInt(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, String setValue, String whereParam, Timestamp whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setString(1, setValue);
    stmt.setTimestamp(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, String setValue, String whereParam, PGgeometry whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setString(1, setValue);
    stmt.setObject(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Integer setValue, String whereParam, String whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setInt(1, setValue);
    stmt.setString(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Integer setValue, String whereParam, Integer whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setInt(1, setValue);
    stmt.setInt(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Integer setValue, String whereParam, Timestamp whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setInt(1, setValue);
    stmt.setTimestamp(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Integer setValue, String whereParam, PGgeometry whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setInt(1, setValue);
    stmt.setObject(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Timestamp setValue, String whereParam, String whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setTimestamp(1, setValue);
    stmt.setString(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Timestamp setValue, String whereParam, Integer whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setTimestamp(1, setValue);
    stmt.setInt(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Timestamp setValue, String whereParam, Timestamp whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setTimestamp(1, setValue);
    stmt.setTimestamp(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, Timestamp setValue, String whereParam, PGgeometry whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setTimestamp(1, setValue);
    stmt.setObject(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, PGgeometry setValue, String whereParam, String whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setObject(1, setValue);
    stmt.setString(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, PGgeometry setValue, String whereParam, Integer whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setObject(1, setValue);
    stmt.setInt(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, PGgeometry setValue, String whereParam, Timestamp whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setObject(1, setValue);
    stmt.setTimestamp(2, whereValue);
    executeSQLStatement(stmt);
  }

  public void update(
      String table, String setParam, PGgeometry setValue, String whereParam, PGgeometry whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "UPDATE " + table + " SET " + setParam + " = ?" + " WHERE " + whereParam + " = ?");
    stmt.setObject(1, setValue);
    stmt.setObject(2, whereValue);
    executeSQLStatement(stmt);
  }

  /**
   * Method for Prepared DELETE SQL statement *
   *
   * @param table - name of table to be deleted
   * @throws SQLException
   */
  public void deleteFrom(String table) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("DELETE FROM " + table);
    executeSQLStatement(stmt);
  }

  /**
   * Method for Prepared DELETE SQL statement with WHERE clause * Method overloaded for every
   * possible type of whereValue
   *
   * @param table -- table to delete an entry from
   * @param whereParam -- name of filtering parameter in WHERE clause
   * @param whereValue -- actual value of filtering parameter
   * @throws SQLException
   */
  public void deleteFromWhere(String table, String whereParam, String whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("DELETE FROM " + table + " WHERE " + whereParam + " = ?");
    stmt.setString(1, whereValue);
    executeSQLStatement(stmt);
  }

  public void deleteFromWhere(String table, String whereParam, Integer whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("DELETE FROM " + table + " WHERE " + whereParam + " = ?");
    stmt.setInt(1, whereValue);
    executeSQLStatement(stmt);
  }

  public void deleteFromWhere(String table, String whereParam, Timestamp whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("DELETE FROM " + table + " WHERE " + whereParam + " = ?");
    stmt.setTimestamp(1, whereValue);
    executeSQLStatement(stmt);
  }

  public void deleteFromWhere(String table, String whereParam, PGgeometry whereValue)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("DELETE FROM " + table + " WHERE " + whereParam + " = ?");
    stmt.setObject(1, whereValue);
    executeSQLStatement(stmt);
  }
}

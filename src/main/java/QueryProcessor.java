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

  /** Prepared SQL Statements methods */
  public void insertInto(String tableName, String... values) throws SQLException {
    PreparedStatement stmt = null;
    tableName = tableName.toUpperCase();
    List<String> args = Arrays.asList(values);
    boolean tableExists = true;

    switch (tableName) {
      case "PLAYER":
        {
          stmt = connection.prepareStatement("insert into PLAYER values(?, ?, ?)");
          stmt.setInt(1, Integer.parseInt(args.get(0)));
          stmt.setString(2, args.get(1));
          stmt.setString(3, args.get(2));
          break;
        }
      case "PLAYER_STATS":
        {
          stmt = connection.prepareStatement("insert into PLAYER_STATS values(?, ?, ?)");
          stmt.setInt(1, Integer.parseInt(args.get(0)));
          stmt.setInt(2, Integer.parseInt(args.get(1)));
          stmt.setInt(3, Integer.parseInt(args.get(2)));
          break;
        }
      case "GALLERY":
        {
          stmt = connection.prepareStatement("insert into GALLERY values(?, ?, ?, ?)");
          stmt.setInt(1, Integer.parseInt(args.get(0)));
          stmt.setTimestamp(2, Timestamp.valueOf(args.get(1)));
          stmt.setInt(3, Integer.parseInt(args.get(2)));
          stmt.setString(4, args.get(3));
          break;
        }
      case "LOCATION":
        {
          stmt = connection.prepareStatement("insert into LOCATION values(?, ?)");
          stmt.setInt(1, Integer.parseInt(args.get(0)));
          stmt.setObject(2, PGgeometry.geomFromString(args.get(1)));
          break;
        }
      case "LANDMARK":
        {
          stmt = connection.prepareStatement("insert into LANDMARK values(?, ?, ?, ?)");
          stmt.setInt(1, Integer.parseInt(args.get(0)));
          stmt.setObject(2, PGgeometry.geomFromString(args.get(1)));
          stmt.setInt(3, Integer.parseInt(args.get(2)));
          stmt.setString(4, args.get(3));
          break;
        }
      case "LANDMARK_TYPE":
        {
          stmt = connection.prepareStatement("insert into LANDMARK_TYPE values(?, ?)");
          stmt.setInt(1, Integer.parseInt(args.get(0)));
          stmt.setString(2, args.get(1));
          break;
        }
      default:
        {
          System.out.println("Table " + tableName + " does not exist");
          tableExists = false;
        }
    }

    if (tableExists) {
      stmt.executeUpdate();
      stmt.close();
    }
  }

  public ResultSet selectFrom(String tableName, String... columns) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("SELECT ? FROM ?");

    String queryColums = getParamSQL(columns);
    stmt.setString(1, queryColums);
    stmt.setString(2, tableName);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, String value, String... arguments) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("SELECT ? FROM ? WHERE ? = ?");

    setBasicSelectParams(stmt, tableName, indexColumn, arguments);
    stmt.setString(4, value);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, Integer value, String... arguments)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("SELECT ? FROM ? WHERE ? = ?");

    setBasicSelectParams(stmt, tableName, indexColumn, arguments);
    stmt.setInt(4, value);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, PGgeometry value, String... arguments)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("SELECT ? FROM ? WHERE ? = ?");

    setBasicSelectParams(stmt, tableName, indexColumn, arguments);
    stmt.setObject(4, value);

    return getResultSet(stmt);
  }

  public ResultSet selectFromWhere(
      String tableName, String indexColumn, Timestamp value, String... arguments)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("SELECT ? FROM ? WHERE ? = ?");

    setBasicSelectParams(stmt, tableName, indexColumn, arguments);
    stmt.setTimestamp(4, value);

    return getResultSet(stmt);
  }

  private void setBasicSelectParams(
      PreparedStatement stmt, String tableName, String indexColumn, String... arguments)
      throws SQLException {
    String queryColumns = getParamSQL(arguments);
    stmt.setString(1, queryColumns);
    stmt.setString(2, tableName);
    stmt.setString(3, indexColumn);
  }

  private String getParamSQL(String... arguments) {
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

  private ResultSet getResultSet(PreparedStatement stmt) throws SQLException {
    ResultSet resultSet = stmt.executeQuery();
    stmt.close();
    return resultSet;
  }

  /**
   * TODO: Implement Update and Delete SQL Methods *
   * public void update(){}
   * public void delete(){}
   */
}

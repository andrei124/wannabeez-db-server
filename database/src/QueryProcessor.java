import org.postgis.PGgeometry;

import java.io.*;
import java.sql.*;
import java.util.*;

public class QueryProcessor {

  private static final String CONFIG_FILEPATH = "database/src/config.properties";

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

  public int insertInto(String tableName, String... values) throws SQLException {
    PreparedStatement stmt = null;
    tableName = tableName.toUpperCase();
    List<String> args = Arrays.asList(values);
    int index = 0;
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
          stmt.setObject(3, Integer.parseInt(args.get(2)));
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
      index = stmt.executeUpdate();
      stmt.close();
    }

    return index;
  }

  public ResultSet selectFrom(String tableName, String... columns) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement("SELECT ? FROM ?");

    List<String> args = Arrays.asList(columns);
    int len = args.size();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      if (i != len - 1) {
        sb.append(args.get(i)).append(",");
      } else {
        sb.append(args.get(i));
      }
    }
    stmt.setString(1, sb.toString());
    stmt.setString(2, tableName);

    ResultSet resultSet = stmt.executeQuery();
    stmt.close();
    return resultSet;
  }

  /** TODO: Implemetn Update and Delete SQL Methods *
   *  public void update(){}
   *  public void delete(){}
   */

}

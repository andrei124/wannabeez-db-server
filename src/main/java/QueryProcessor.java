import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import org.postgis.PGgeometry;

import javax.swing.plaf.nimbus.State;

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
      System.out.println("Exception occurred when loading the config file");
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
  public void addNewPlayer(String email, String password) throws SQLException {
    insert("Player", email, password);
  }

  private void insert(String tableName, String email, String password) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "insert into " + tableName + " (\"email\", \"password\") " + " values(?, ?)");
    stmt.setString(1, email);
    stmt.setString(2, password);
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for PLAYER_STATS * */
  public void addPlayerStats(Integer player_id, Integer xp, Integer cash) throws SQLException {
    insert("Player_Stats", player_id, xp, cash);
  }

  private void insert(String tableName, Integer player_id, Integer xp, Integer cash)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " values(?, ?, ?)");
    stmt.setInt(1, player_id);
    stmt.setInt(2, xp);
    stmt.setInt(3, cash);
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for GALLERY * */
  public Integer addNewImageMetaData(Timestamp ts, Integer playerId, String url)
      throws SQLException {
    return insert("Gallery", ts, playerId, url);
  }

  private Integer insert(String tableName, Timestamp ts, Integer playerId, String url)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "insert into "
                + tableName
                + " (\"ts\", \"player_id\", \"url\") "
                + " values(?, ?, ?) returning id");
    stmt.setTimestamp(1, ts);
    stmt.setInt(2, playerId);
    stmt.setString(3, url);

    ResultSet id = stmt.executeQuery();
    id.next();
    return id.getInt("id");
  }

  /** Insert SQL Prepared statement for LOCATION * */
  public void addNewLocation(Integer imageId, Float latitude, Float longitude) throws SQLException {
    insert("Location", imageId, latitude, longitude);
  }

  private void insert(String tableName, Integer imageId, Float latitude, Float longitude)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "insert into "
                + tableName
                + " values(?, "
                + "ST_SetSRID(ST_MakePoint("
                + longitude
                + ", "
                + latitude
                + "), 4326)::geography::geometry)");
    stmt.setInt(1, imageId);
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for LANDMARK * */
  public void addNewLandmark(Float latitude, Float longitude, Integer type, String description)
      throws SQLException {
    insert("Landmark", latitude, longitude, type, description);
  }

  private void insert(
      String tableName, Float latitude, Float longitude, Integer type, String description)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "insert into "
                + tableName
                + " (\"location\", \"type\", \"description\") "
                + "values(ST_SetSRID(ST_MakePoint("
                + longitude
                + ", "
                + latitude
                + "),4326)"
                + ", ?, ?)");
    stmt.setInt(1, type);
    stmt.setString(2, description);
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared statement for LANDMARK_TYPE * */
  public void addNewLandmarkType(String name) throws SQLException {
    insert("Landmark_Type", name);
  }

  private void insert(String tableName, String name) throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement("insert into " + tableName + " (\"name\") " + " values(?)");
    stmt.setString(1, name);
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared Statement for QUEST * */
  public void addNewQuest(Integer type, String name, String description) throws SQLException {
    insert("Quest", type, name, description);
  }

  private void insert(String tableName, Integer type, String name, String description)
      throws SQLException {
    PreparedStatement stmt =
        connection.prepareStatement(
            "insert into "
                + tableName
                + " (\"type\", \"name\", \"description\") "
                + " values(?, ?, ?)");
    stmt.setInt(1, type);
    stmt.setString(2, name);
    stmt.setString(3, description);
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  /** Insert SQL Prepared Statements for QUEST_TYPE * */
  public void addNewQuestType(String name) throws SQLException {
    insert("Quest_Type", name);
  }

  /** Insert SQL Prepared Statement for QUEST_LOCATION * */
  public void addNewQuestLocation(Integer questId, Float latitude, Float longitude)
      throws SQLException {
    insert("Quest_Location", questId, latitude, longitude);
  }

  /**
   * Method for SQL SELECT Query
   *
   * @param columns -- attributes to be selected
   * @return SelectQueryBuilder -- to be used in order to perform the query
   */
  public SelectQueryBuilder select(String... columns) {
    return new SelectQueryBuilder(connection, columns);
  }

  /**
   * Method for SQL UPDATE Statement
   *
   * @param table -- name of table to be updated
   * @return -- UpdateStatementBuilder -- to be used in order to execute the Update statement
   */
  public UpdateStatementBuilder update(String table) {
    return new UpdateStatementBuilder(connection, table);
  }

  /**
   * Method for SQL DELETE Statement
   *
   * @return DeleteStatementBuilder -- to be used in order to perform the Delete
   */
  public DeleteStatementBuilder delete() {
    return new DeleteStatementBuilder(connection);
  }
}

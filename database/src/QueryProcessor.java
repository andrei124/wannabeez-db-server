import java.io.*;
import java.sql.*;
import java.util.Properties;

public class QueryProcessor {

  private static final String CONFIG_FILEPATH = "database/src/config.properties";

  private File configFile;
  private FileReader fileReader;
  private Properties properties;
  private Connection connection;

  public QueryProcessor() throws ClassNotFoundException, IOException {
    /* Do not connect by default upon creation */
    connection = null;

    configFile = new File(CONFIG_FILEPATH);
    properties = new Properties();

    fileReader = new FileReader(configFile);
    properties.load(fileReader);

    String driver = properties.getProperty("driver");
    registerDriver(driver);
  }

  private void registerDriver(String driver) throws ClassNotFoundException {
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
    if(connection != null) {
      try {
        connection.close();
        System.out.println("Connection succesfully closed");
      } catch (SQLException e) {
        System.out.println("Error occured when closing connection");
        e.printStackTrace();
      }
    }
  }


}

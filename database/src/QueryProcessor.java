import java.io.*;
import java.sql.*;
import java.util.Properties;

public class QueryProcessor {

  private File configFile;
  private FileReader fileReader;
  private Properties properties;

  public QueryProcessor() throws ClassNotFoundException, IOException {
    configFile = new File("database/src/config.properties");
    properties = new Properties();

    fileReader = new FileReader(configFile);
    properties.load(fileReader);

    /** TODO: Register the JDBC Driver
     String driver = properties.getProperty("driver");
     registerDriver(driver);
     */
  }

  private void registerDriver(String driver) throws ClassNotFoundException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      System.out.println("JDBC Driver could not be registered");
      System.exit(1);
      e.printStackTrace();
    }
  }

  public void connect() {
    Connection connection = null;
    String url = properties.getProperty("url");
    String user = properties.getProperty("user");
    String password = properties.getProperty("password");


    /** TODO: create connection to Database
     *  try {
        connection = DriverManager.getConnection(url, user, password);
    } catch (SQLException e) {
        System.out.println("SQL Exception: " + e.getMessage());
    } */

  }
}

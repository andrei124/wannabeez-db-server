import java.sql.SQLException;

public class QuerySession {

  public static void main(String[] args) throws SQLException {

    QueryProcessor queryProcessor = new QueryProcessor();
    queryProcessor.connect();
    queryProcessor.closeConnection();
  }

}

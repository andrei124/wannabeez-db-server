import java.sql.ResultSet;
import java.sql.SQLException;

public class QuerySession {

  public static void main(String[] args) throws SQLException {

    QueryProcessor queryProcessor = new QueryProcessor();
    queryProcessor.connect();

    ResultSet rs = queryProcessor.selectFrom("LOCATION", "location");
    if (!rs.next()) {
      System.out.println("No records found");
    }

    queryProcessor.closeConnection();
  }
}

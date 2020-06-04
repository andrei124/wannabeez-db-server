import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class QueryHelpers {

  /**
   * Helper method for executing an SQL Prepared Statement
   *
   * @param stmt -- prepared statement to be executed
   * @throws SQLException
   */
  public static void executeSQLStatement(PreparedStatement stmt) throws SQLException {
    stmt.executeUpdate();
  }

  /**
   * Helper method for retrieving the result of an SQL query *
   *
   * @param stmt - statement to retrieve the result for
   * @return
   * @throws SQLException
   */
  public static ResultSet getResultSet(PreparedStatement stmt) throws SQLException {
    return stmt.executeQuery();
  }

  /**
   * Helper method for getting the columns to be queried into the SELECT statement *
   *
   * @param arguments - list of column names to be selected
   * @return
   */
  public static String getColumnsToBeQueried(String... arguments) {
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
}

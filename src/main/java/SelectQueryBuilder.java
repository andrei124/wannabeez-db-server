import org.postgis.PGgeometry;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class SelectQueryBuilder {

  private StringBuilder sqlSelectQuery;
  private Connection connection;

  public SelectQueryBuilder(Connection connection, String... columns) {
    this.connection = connection;
    sqlSelectQuery = new StringBuilder();
    sqlSelectQuery.append("SELECT ").append(getColumnsToBeQueried(columns)).append(" ");
  }

  public SelectQueryBuilder from(String table) {
    sqlSelectQuery.append("FROM ").append(table);
    return this;
  }

  public SelectQueryBuilder where(String whereParam) {
    sqlSelectQuery.append(" WHERE " + whereParam + " = ");
    return this;
  }

  public SelectQueryBuilder is(String value) {
    sqlSelectQuery.append(value);
    return this;
  }

  public SelectQueryBuilder is(Integer value) {
    sqlSelectQuery.append(value);
    return this;
  }

  public SelectQueryBuilder is(Timestamp value) {
    sqlSelectQuery.append(value);
    return this;
  }

  public SelectQueryBuilder is(PGgeometry value) {
    sqlSelectQuery.append(value);
    return this;
  }

  public ResultSet execute() throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sqlSelectQuery.toString());
    return getResultSet(stmt);
  }

  /**
   * Helper method for retrieving the result of an SQL query *
   *
   * @param stmt - statement to retrieve the result for
   * @return
   * @throws SQLException
   */
  private static ResultSet getResultSet(PreparedStatement stmt) throws SQLException {
    return stmt.executeQuery();
  }

  /**
   * Helper method for getting the columns to be queried into the SELECT statement *
   *
   * @param arguments - list of column names to be selected
   * @return
   */
  private static String getColumnsToBeQueried(String... arguments) {
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

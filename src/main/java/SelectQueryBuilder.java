import org.postgis.PGgeometry;
import java.sql.*;

public class SelectQueryBuilder {

  private StringBuilder sqlSelectQuery;
  private Connection connection;

  public SelectQueryBuilder(Connection connection, String... columns) {
    this.connection = connection;
    sqlSelectQuery = new StringBuilder();
    sqlSelectQuery
        .append("SELECT ")
        .append(QueryHelpers.getColumnsToBeQueried(columns))
        .append(" ");
  }

  public SelectQueryBuilder from(String table) {
    sqlSelectQuery.append("FROM ").append(table);
    return this;
  }

  public SelectQueryBuilder where(String whereParam) {
    sqlSelectQuery.append(" WHERE ").append(whereParam).append(" = ");
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
    return QueryHelpers.getResultSet(stmt);
  }
}

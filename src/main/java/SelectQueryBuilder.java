import org.postgis.PGgeometry;
import java.sql.*;

public class SelectQueryBuilder {

  private StringBuilder sqlSelectQuery = new StringBuilder();
  private PreparedStatement stmt = null;
  private Connection connection;

  public SelectQueryBuilder(Connection connection, String... columns) {
    this.connection = connection;
    sqlSelectQuery
        .append("SELECT ")
        .append(QueryHelpers.getColumnsToBeQueried(columns))
        .append(" ");
  }

  public SelectQueryBuilder from(String table) {
    sqlSelectQuery.append("FROM ").append(table);
    return this;
  }

  public SelectQueryBuilder where(String whereParam) throws SQLException {
    sqlSelectQuery.append(" WHERE ").append(whereParam).append(" = ?");
    stmt = connection.prepareStatement(sqlSelectQuery.toString());
    return this;
  }

  public SelectQueryBuilder is(String value) throws SQLException {
    stmt.setString(1, value);
    return this;
  }

  public SelectQueryBuilder is(Integer value) throws SQLException {
    stmt.setInt(1, value);
    return this;
  }

  public SelectQueryBuilder is(Timestamp value) throws SQLException {
    stmt.setTimestamp(1, value);
    return this;
  }

  public SelectQueryBuilder is(PGgeometry value) throws SQLException {
    stmt.setObject(1, value);
    return this;
  }

  public ResultSet execute() throws SQLException {
    if (!sqlSelectQuery.toString().contains("WHERE")) {
      stmt = connection.prepareStatement(sqlSelectQuery.toString());
    }
    return QueryHelpers.getResultSet(stmt);
  }
}

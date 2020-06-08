import org.postgis.PGgeometry;
import java.sql.*;

public class SelectQueryBuilder implements WhereClauseBuilder {

  private StringBuilder sqlSelectQuery = new StringBuilder();
  private PreparedStatement stmt = null;
  private Connection connection;

  public SelectQueryBuilder(Connection connection, String... columns) {
    this.connection = connection;
    sqlSelectQuery
        .append("SELECT ")
        .append(DBInterfaceHelpers.getColumnsToBeQueried(columns))
        .append(" ");
  }

  public SelectQueryBuilder from(String table) {
    sqlSelectQuery.append("FROM ").append(table);
    return this;
  }

  @Override
  public SelectQueryBuilder where(String whereParam) throws SQLException {
    sqlSelectQuery.append(" WHERE ").append(whereParam).append(" = ?");
    stmt = connection.prepareStatement(sqlSelectQuery.toString());
    return this;
  }

  public SelectQueryBuilder whereSTContains(Double x, Double y, Double radius) throws SQLException {
    sqlSelectQuery
        .append(" WHERE ")
        .append("ST_Contains(ST_Buffer(ST_MakePoint(")
        .append(x)
        .append(", ")
        .append(y)
        .append(")")
        .append("::geography, ")
        .append(radius)
        .append(")::geometry, Landmark.location)");
    stmt = connection.prepareStatement(sqlSelectQuery.toString());
    return this;
  }

  @Override
  public SelectQueryBuilder is(String value) throws SQLException {
    stmt.setString(1, value);
    return this;
  }

  @Override
  public SelectQueryBuilder is(Integer value) throws SQLException {
    stmt.setInt(1, value);
    return this;
  }

  @Override
  public SelectQueryBuilder is(Timestamp value) throws SQLException {
    stmt.setTimestamp(1, value);
    return this;
  }

  @Override
  public SelectQueryBuilder is(PGgeometry value) throws SQLException {
    stmt.setObject(1, value);
    return this;
  }

  @Override
  public ResultSet executeSelect() throws SQLException {
    if (!sqlSelectQuery.toString().contains("WHERE")) {
      stmt = connection.prepareStatement(sqlSelectQuery.toString());
    }
    return DBInterfaceHelpers.getResultSet(stmt);
  }

  @Override
  public StringBuilder getSQLStatement() {
    return sqlSelectQuery;
  }

  @Override
  public void execute() throws SQLException {
    // Does nothing
  }
}

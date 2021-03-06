import org.postgis.PGgeometry;

import java.sql.*;

public class UpdateStatementBuilder implements WhereClauseBuilder {

  private StringBuilder sqlUpdateStatement = new StringBuilder();
  private PreparedStatement stmt = null;
  private Connection connection;

  private String toString = null;
  private Integer toInt = null;
  private Timestamp toTimestamp = null;
  private PGgeometry toPGeometry = null;

  public UpdateStatementBuilder(Connection connection, String table) {
    this.connection = connection;
    sqlUpdateStatement.append("UPDATE ").append(table);
  }

  public UpdateStatementBuilder set(String column) {
    sqlUpdateStatement.append(" SET ").append(column).append(" = ?");
    return this;
  }

  public UpdateStatementBuilder to(String value) {
    toString = value;
    return this;
  }

  public UpdateStatementBuilder to(Integer value) {
    toInt = value;
    return this;
  }

  public UpdateStatementBuilder to(Timestamp value) {
    toTimestamp = value;
    return this;
  }

  public UpdateStatementBuilder to(PGgeometry value) {
    toPGeometry = value;
    return this;
  }

  @Override
  public UpdateStatementBuilder where(String whereParam) throws SQLException {
    sqlUpdateStatement.append(" WHERE ").append(whereParam).append(" = ?");
    stmt = connection.prepareStatement(sqlUpdateStatement.toString());
    return this;
  }

  @Override
  public UpdateStatementBuilder is(String value) throws SQLException {
    setFirstStatementParam();
    stmt.setString(2, value);
    return this;
  }

  @Override
  public UpdateStatementBuilder is(Integer value) throws SQLException {
    setFirstStatementParam();
    stmt.setInt(2, value);
    return this;
  }

  @Override
  public UpdateStatementBuilder is(Timestamp value) throws SQLException {
    setFirstStatementParam();
    stmt.setTimestamp(2, value);
    return this;
  }

  @Override
  public UpdateStatementBuilder is(PGgeometry value) throws SQLException {
    setFirstStatementParam();
    stmt.setObject(2, value);
    return this;
  }

  @Override
  public void execute() throws SQLException {
    DBInterfaceHelpers.executeSQLStatement(stmt);
  }

  public StringBuilder getSQLStatement() {
    return sqlUpdateStatement;
  }

  private void setFirstStatementParam() throws SQLException {
    if (toString != null) {
      stmt.setString(1, toString);
    } else if (toInt != null) {
      stmt.setInt(1, toInt);
    } else if (toTimestamp != null) {
      stmt.setTimestamp(1, toTimestamp);
    } else if (toPGeometry != null) {
      stmt.setObject(1, toPGeometry);
    }
  }

  @Override
  public ResultSet executeSelect() throws SQLException {
    return null;
  }
}

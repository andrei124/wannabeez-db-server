import org.postgis.PGgeometry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class DeleteStatementBuilder implements WhereClauseBuilder {

  private StringBuilder sqlDeleteStatement = new StringBuilder();
  private PreparedStatement stmt = null;
  private Connection connection;

  public DeleteStatementBuilder(Connection connection) {
    this.connection = connection;
    sqlDeleteStatement.append("DELETE ");
  }

  public DeleteStatementBuilder from(String table) {
    sqlDeleteStatement.append("FROM ").append(table);
    return this;
  }

  @Override
  public DeleteStatementBuilder where(String whereParam) throws SQLException {
    sqlDeleteStatement.append(" WHERE ").append(whereParam).append(" = ?");
    stmt = connection.prepareStatement(sqlDeleteStatement.toString());
    return this;
  }

  @Override
  public DeleteStatementBuilder is(String value) throws SQLException {
    stmt.setString(1, value);
    return this;
  }

  @Override
  public DeleteStatementBuilder is(Integer value) throws SQLException {
    stmt.setInt(1, value);
    return this;
  }

  @Override
  public DeleteStatementBuilder is(Timestamp value) throws SQLException {
    stmt.setTimestamp(1, value);
    return this;
  }

  @Override
  public DeleteStatementBuilder is(PGgeometry value) throws SQLException {
    stmt.setObject(1, value);
    return this;
  }

  @Override
  public void execute() throws SQLException {
    if (!sqlDeleteStatement.toString().contains("WHERE")) {
      stmt = connection.prepareStatement(sqlDeleteStatement.toString());
    }
    QueryHelpers.executeSQLStatement(stmt);
  }

  @Override
  public StringBuilder getSQLStatement() {
    return sqlDeleteStatement;
  }
}

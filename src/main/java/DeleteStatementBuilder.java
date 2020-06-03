import org.postgis.PGgeometry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class DeleteStatementBuilder {

  private StringBuilder sqlDeleteStatement;
  private Connection connection;

  public DeleteStatementBuilder(Connection connection) {
    this.connection = connection;
    sqlDeleteStatement = new StringBuilder();
    sqlDeleteStatement.append("DELETE ");
  }

  public DeleteStatementBuilder from(String table) {
    sqlDeleteStatement.append("FROM ").append(table);
    return this;
  }

  public DeleteStatementBuilder where(String whereParam) {
    sqlDeleteStatement.append(" WHERE ").append(whereParam).append(" = ");
    return this;
  }

  public DeleteStatementBuilder is(String value) {
    sqlDeleteStatement.append(value);
    return this;
  }

  public DeleteStatementBuilder is(Integer value) {
    sqlDeleteStatement.append(value);
    return this;
  }

  public DeleteStatementBuilder is(Timestamp value) {
    sqlDeleteStatement.append(value);
    return this;
  }

  public DeleteStatementBuilder is(PGgeometry value) {
    sqlDeleteStatement.append(value);
    return this;
  }

  public void execute() throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sqlDeleteStatement.toString());
    QueryHelpers.executeSQLStatement(stmt);
  }
}

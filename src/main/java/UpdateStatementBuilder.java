import org.postgis.PGgeometry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class UpdateStatementBuilder {
    private StringBuilder sqlUpdateStatement;
    private String table;
    private Connection connection;

    public UpdateStatementBuilder(Connection connection, String table) {
        this.table = table;
        this.connection = connection;
        sqlUpdateStatement = new StringBuilder();
        sqlUpdateStatement.append("UPDATE ").append(table);
    }

    public UpdateStatementBuilder set(String column) {
        sqlUpdateStatement.append(" SET ").append(column).append(" = ");
        return this;
    }

    public UpdateStatementBuilder to(String value) {
        sqlUpdateStatement.append("'").append(value).append("'");
        return this;
    }

    public UpdateStatementBuilder to(Integer value) {
        sqlUpdateStatement.append(value);
        return this;
    }

    public UpdateStatementBuilder to(Timestamp value) {
        sqlUpdateStatement.append(value);
        return this;
    }

    public UpdateStatementBuilder to(PGgeometry value) {
        sqlUpdateStatement.append(value);
        return this;
    }

    public UpdateStatementBuilder where(String whereParam) {
        sqlUpdateStatement.append(" WHERE ").append(whereParam).append(" = ");
        return this;
    }

    public UpdateStatementBuilder is(String value) {
        sqlUpdateStatement.append("'").append(value).append("'");
        return this;
    }

    public UpdateStatementBuilder is(Integer value) {
        sqlUpdateStatement.append(value);
        return this;
    }

    public UpdateStatementBuilder is(Timestamp value) {
        sqlUpdateStatement.append(value);
        return this;
    }

    public UpdateStatementBuilder is(PGgeometry value) {
        sqlUpdateStatement.append(value);
        return this;
    }

    public void execute() throws SQLException {
        System.out.println("The update statement is:   " + sqlUpdateStatement.toString());
        PreparedStatement stmt = connection.prepareStatement(sqlUpdateStatement.toString());
        QueryHelpers.executeSQLStatement(stmt);
    }
}

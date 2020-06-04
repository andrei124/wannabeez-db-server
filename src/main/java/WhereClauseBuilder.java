import org.postgis.PGgeometry;

import java.sql.SQLException;
import java.sql.Timestamp;

public interface WhereClauseBuilder {

  WhereClauseBuilder where(String whereParam) throws SQLException;

  WhereClauseBuilder is(String value) throws SQLException;

  WhereClauseBuilder is(Integer value) throws SQLException;

  WhereClauseBuilder is(Timestamp value) throws SQLException;

  WhereClauseBuilder is(PGgeometry value) throws SQLException;

  void execute() throws SQLException;

  StringBuilder getSQLStatement();
}

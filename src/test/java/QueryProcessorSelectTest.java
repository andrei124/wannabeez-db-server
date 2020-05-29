import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

public class QueryProcessorSelectTest {

  @Rule public JUnitRuleMockery context = new JUnitRuleMockery();
  Connection mockJDBCconnection = context.mock(Connection.class);

  final Timestamp exampleTS = new Timestamp(2020, 5, 29, 7, 5, 5, 6);
  final PGgeometry examplePG = new PGgeometry(new Point(12, 34));
  final String exampleSTR = "Example";
  QueryProcessor queryProcessor = new QueryProcessor(mockJDBCconnection);

  @Test
  public void selectStatementParsedCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("SELECT ? FROM ?");
          }
        });
    queryProcessor.selectFrom("Player", "id");
  }

  @Test
  public void selectStatementWithWhereClauseIntegerParsedCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("SELECT ? FROM ? WHERE ? = ?");
          }
        });
    queryProcessor.selectFromWhere("Player", "id", 1, "*");
  }

  @Test
  public void selectStatementWithWhereClausePGeometryParsedCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("SELECT ? FROM ? WHERE ? = ?");
          }
        });
    queryProcessor.selectFromWhere("Location", "location", examplePG, "id");
  }

  @Test
  public void selectStatementWithWhereClauseTimestampParsedCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("SELECT ? FROM ? WHERE ? = ?");
          }
        });
    queryProcessor.selectFromWhere("Gallery", "ts", exampleTS, "player_id,url");
  }

  @Test
  public void selectStatementWithWhereClauseStringParsedCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("SELECT ? FROM ? WHERE ? = ?");
          }
        });
    queryProcessor.selectFromWhere("Landmark", "description", exampleSTR, "type");
  }
}

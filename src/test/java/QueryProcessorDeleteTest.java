import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

public class QueryProcessorDeleteTest {

  @Rule public JUnitRuleMockery context = new JUnitRuleMockery();
  Connection mockJDBCconnection = context.mock(Connection.class);

  Timestamp exampleTS = new Timestamp(2020, 5, 29, 7, 5, 5, 6);
  PGgeometry examplePG = new PGgeometry(new Point(12, 34));
  QueryProcessor queryProcessor = new QueryProcessor(mockJDBCconnection);

  @Test
  public void deleteStatementParsedCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("DELETE FROM Player");
          }
        });
    queryProcessor.deleteFrom("Player");
  }

  @Test
  public void deleteWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("DELETE FROM Player WHERE email = ?");
          }
        });
    queryProcessor.deleteFromWhere("Player", "email", "example@email.com");
  }

  @Test
  public void deleteWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("DELETE FROM Landmark_Type WHERE id = ?");
          }
        });
    queryProcessor.deleteFromWhere("Landmark_Type", "id", 7);
  }

  @Test
  public void deleteWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("DELETE FROM Gallery WHERE ts = ?");
          }
        });
    queryProcessor.deleteFromWhere("Gallery", "ts", exampleTS);
  }

  @Test
  public void deleteWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("DELETE FROM Location WHERE location = ?");
          }
        });
    queryProcessor.deleteFromWhere("Location", "location", examplePG);
  }
}

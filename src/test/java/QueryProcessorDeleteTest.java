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
    queryProcessor.delete().from("Player").execute();
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
    queryProcessor.delete().from("Player").where("email").is("example@email.com").execute();
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
    queryProcessor.delete().from("Landmark_Type").where("id").is(7).execute();
  }

  @Test
  public void deleteWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("DELETE FROM Gallery WHERE ts = ?");
          }
        });
    queryProcessor.delete().from("Gallery").where("ts").is(exampleTS).execute();
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
    queryProcessor.delete().from("Location").where("location").is(examplePG).execute();
  }
}

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

public class QueryProcessorUpdateTest {

  @Rule public JUnitRuleMockery context = new JUnitRuleMockery();
  Connection mockJDBCconnection = context.mock(Connection.class);

  Timestamp exampleTS = new Timestamp(2020, 5, 29, 7, 5, 5, 6);
  PGgeometry examplePG = new PGgeometry(new Point(12, 34));
  QueryProcessor queryProcessor = new QueryProcessor(mockJDBCconnection);

  @Test
  public void updateSetStringWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Player", "email", "example@email.com", "password", "example123");
  }

  @Test
  public void updateSetStringWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Player", "email", "example@email.com", "id", 123);
  }

  @Test
  public void updateSetStringWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Gallery", "url", "basicURL", "ts", exampleTS);
  }

  @Test
  public void updateSetStringWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Landmark", "description", "Sample Description", "location", examplePG);
  }

  @Test
  public void updateSetIntegerWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Player", "id", 20, "email", "example123@example.com");
  }

  @Test
  public void updateSetIntegerWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Player_Stats", "xp", 100, "id", 20);
  }

  @Test
  public void updateSetIntegerWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Gallery", "player_id", 1253, "ts", exampleTS);
  }

  @Test
  public void updateSetIntegerWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Location", "id", 13618, "location", examplePG);
  }

  @Test
  public void updateSetTimestampWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Gallery", "ts", exampleTS, "url", "example_url");
  }

  @Test
  public void updateSetTimestampWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Gallery", "ts", exampleTS, "id", 1231);
  }

  @Test
  public void updateSetTimestampWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Gallery", "ts", exampleTS, "ts", exampleTS);
  }

  @Test
  public void updateSetTimestampWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("MOCK_TABLE", "ts", exampleTS, "location", examplePG);
  }

  @Test
  public void updateSetPGeometryWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Landmark", "location", examplePG, "description", "Example Description");
  }

  @Test
  public void updateSetPGeometryWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Landmark", "location", examplePG, "id", 56);
  }

  @Test
  public void updateSetPGeometryWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("MOCK_TABLE", "location", examplePG, "ts", exampleTS);
  }

  @Test
  public void updateSetPGeometryWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("UPDATE ? SET ? = ? WHERE ? = ?");
          }
        });
    queryProcessor.update("Landmark", "location", examplePG, "location", examplePG);
  }
}

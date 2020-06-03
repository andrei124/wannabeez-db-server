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
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Player SET email = 'example@email.com' WHERE password = 'example123'");
          }
        });
    queryProcessor.update("Player").set("email").to("example@email.com").where("password").is("example123").execute();
  }

  @Test
  public void updateSetStringWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Player SET email = 'example@email.com' WHERE id = 123");
          }
        });
    queryProcessor.update("Player").set("email").to("example@email.com").where("id").is(123).execute();
  }

  @Test
  public void updateSetStringWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Gallery SET url = 'example_url' WHERE ts = " + exampleTS.toString());
          }
        });
    queryProcessor.update("Gallery").set("url").to("example_url").where("ts").is(exampleTS).execute();
  }

  @Test
  public void updateSetStringWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Landmark SET description = 'Sample Description' WHERE location = " + examplePG.toString());
          }
        });
    queryProcessor.update("Landmark").set("description").to("Sample Description").where("location").is(examplePG).execute();
  }

  @Test
  public void updateSetIntegerWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Player SET id = ? WHERE email = ?");
          }
        });
    queryProcessor.update("Player", "id", 20, "email", "example123@example.com");
  }

  @Test
  public void updateSetIntegerWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Player_Stats SET xp = ? WHERE id = ?");
          }
        });
    queryProcessor.update("Player_Stats", "xp", 100, "id", 20);
  }

  @Test
  public void updateSetIntegerWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Gallery SET player_id = ? WHERE ts = ?");
          }
        });
    queryProcessor.update("Gallery", "player_id", 1253, "ts", exampleTS);
  }

  @Test
  public void updateSetIntegerWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Location SET id = ? WHERE location = ?");
          }
        });
    queryProcessor.update("Location", "id", 13618, "location", examplePG);
  }

  @Test
  public void updateSetTimestampWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Gallery SET ts = ? WHERE url = ?");
          }
        });
    queryProcessor.update("Gallery", "ts", exampleTS, "url", "example_url");
  }

  @Test
  public void updateSetTimestampWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Gallery SET ts = ? WHERE id = ?");
          }
        });
    queryProcessor.update("Gallery", "ts", exampleTS, "id", 1231);
  }

  @Test
  public void updateSetTimestampWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Gallery SET ts = ? WHERE ts = ?");
          }
        });
    queryProcessor.update("Gallery", "ts", exampleTS, "ts", exampleTS);
  }

  /**
   * Test performed on a MOCK_TABLE since the combination (setValue, whereValue) = (Timestamp,
   * PGeometry) does not currently exists in any of our actual DB Created for potential extension of
   * our DB requirements and for Unit Testing only
   *
   * @throws SQLException
   */
  @Test
  public void updateSetTimestampWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE MOCK_TABLE SET ts = ? WHERE location = ?");
          }
        });
    queryProcessor.update("MOCK_TABLE", "ts", exampleTS, "location", examplePG);
  }

  @Test
  public void updateSetPGeometryWhereStringHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Landmark SET location = ? WHERE description = ?");
          }
        });
    queryProcessor.update("Landmark", "location", examplePG, "description", "Example Description");
  }

  @Test
  public void updateSetPGeometryWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Landmark SET location = ? WHERE id = ?");
          }
        });
    queryProcessor.update("Landmark", "location", examplePG, "id", 56);
  }

  /**
   * Test performed on a MOCK_TABLE since the combination (setValue, whereValue) = (PGeometry,
   * Timestamp) does not currently exists in any of our actual DB Created for potential extension of
   * our DB requirements and for Unit Testing only
   *
   * @throws SQLException
   */
  @Test
  public void updateSetPGeometryWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE MOCK_TABLE SET location = ? WHERE ts = ?");
          }
        });
    queryProcessor.update("MOCK_TABLE", "location", examplePG, "ts", exampleTS);
  }

  @Test
  public void updateSetPGeometryWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Landmark SET location = ? WHERE location = ?");
          }
        });
    queryProcessor.update("Landmark", "location", examplePG, "location", examplePG);
  }
}

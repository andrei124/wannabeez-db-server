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
                .prepareStatement("UPDATE Player SET email = ? WHERE password = ?");
          }
        });
    queryProcessor
        .update("Player")
        .set("email")
        .to("example@email.com")
        .where("password")
        .is("example123")
        .execute();
  }

  @Test
  public void updateSetStringWhereIntegerHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Player SET email = ? WHERE id = ?");
          }
        });
    queryProcessor
        .update("Player")
        .set("email")
        .to("example@email.com")
        .where("id")
        .is(123)
        .execute();
  }

  @Test
  public void updateSetStringWhereTimestampHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Gallery SET url = ? WHERE ts = ?");
          }
        });
    queryProcessor
        .update("Gallery")
        .set("url")
        .to("example_url")
        .where("ts")
        .is(exampleTS)
        .execute();
  }

  @Test
  public void updateSetStringWherePGeometryHandledCorrectly() throws SQLException {
    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("UPDATE Landmark SET description = ? WHERE location = ?");
          }
        });
    queryProcessor
        .update("Landmark")
        .set("description")
        .to("Sample Description")
        .where("location")
        .is(examplePG)
        .execute();
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
    queryProcessor
        .update("Player")
        .set("id")
        .to(20)
        .where("email")
        .is("example123@example.com")
        .execute();
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
    queryProcessor.update("Player_Stats").set("xp").to(100).where("id").is(20).execute();
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
    queryProcessor.update("Gallery").set("player_id").to(1253).where("ts").is(exampleTS);
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
    queryProcessor.update("Location").set("id").to(13618).where("location").is(examplePG).execute();
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
    queryProcessor
        .update("Gallery")
        .set("ts")
        .to(exampleTS)
        .where("url")
        .is("example_url")
        .execute();
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
    queryProcessor.update("Gallery").set("ts").to(exampleTS).where("id").is(1231).execute();
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
    queryProcessor.update("Gallery").set("ts").to(exampleTS).where("ts").is(exampleTS);
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
    queryProcessor
        .update("Landmark")
        .set("location")
        .to(examplePG)
        .where("description")
        .is("Example Description")
        .execute();
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
    queryProcessor.update("Landmark").set("location").to(examplePG).where("id").is(56).execute();
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
    queryProcessor
        .update("Landmark")
        .set("location")
        .to(examplePG)
        .where("location")
        .is(examplePG)
        .execute();
  }
}

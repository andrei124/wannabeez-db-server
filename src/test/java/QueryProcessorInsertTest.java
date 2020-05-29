import org.junit.Rule;
import org.junit.Test;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

public class QueryProcessorInsertTest {

  @Rule public JUnitRuleMockery context = new JUnitRuleMockery();
  Connection mockJDBCconnection = context.mock(Connection.class);

  Timestamp exampleTS = new Timestamp(2020, 5, 29, 7, 5, 5, 6);
  PGgeometry examplePG = new PGgeometry(new Point(12, 34));
  QueryProcessor queryProcessor = new QueryProcessor(mockJDBCconnection);

  @Test
  public void insertIntoPlayerTableUpdatesDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into PLAYER values(?, ?, ?)");
          }
        });

    queryProcessor.insertInto("Player", "100", "email", "password");
  }

  @Test
  public void insertIntoGalleryTableUpdatesDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into GALLERY values(?, ?, ?, ?)");
          }
        });

    queryProcessor.insertInto("GalleRY", "1", exampleTS.toString(), "2", "Description");
  }

  @Test
  public void insertIntoPlayerStatsTableUpdatesDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into PLAYER_STATS values(?, ?, ?)");
          }
        });

    queryProcessor.insertInto("player_STATS", "12", "102", "1236");
  }

  @Test
  public void insertIntoLocationTableUpdatesDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into LOCATION values(?, ?)");
          }
        });

    queryProcessor.insertInto("LOCATION", "1", examplePG.toString());
  }

  @Test
  public void insertIntoLandmarkTableUpdatesDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into LANDMARK values(?, ?, ?, ?)");
          }
        });

    queryProcessor.insertInto("LANDMARK", "1", examplePG.toString(), "6", "Example Landmark");
  }

  @Test
  public void insertIntoLandmarkTypeTableUpdatesDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into LANDMARK_TYPE values(?, ?)");
          }
        });

    queryProcessor.insertInto("landmark_type", "1", "Example Landmark Type");
  }

  @Test
  public void insertIntoInexistentTableDoesNotUpdateDB() throws SQLException {

    context.checking(
        new Expectations() {
          {
            never(mockJDBCconnection);
          }
        });

    queryProcessor.insertInto("RANDOM_TABLE", "1231adadads", "ajshfjagh");
  }
}

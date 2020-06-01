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
  public void insertIntoPlayerTableParsedCorretly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into ? values(?, ?, ?)");
          }
        });

    queryProcessor.insert("Player", 100, "email", "password");
  }

  @Test
  public void insertIntoGalleryTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into ? values(?, ?, ?, ?)");
          }
        });

    queryProcessor.insert("GalleRY", 1, exampleTS, 2, "Description");
  }

  @Test
  public void insertIntoPlayerStatsTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into ? values(?, ?, ?)");
          }
        });

    queryProcessor.insert("player_STATS", 12, 102, 1236);
  }

  @Test
  public void insertIntoLocationTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into ? values(?, ?)");
          }
        });

    queryProcessor.insert("LOCATION", 1, examplePG);
  }

  @Test
  public void insertIntoLandmarkTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into ? values(?, ?, ?, ?)");
          }
        });

    queryProcessor.insert("LANDMARK", 1, examplePG, 6, "Example Landmark");
  }

  @Test
  public void insertIntoLandmarkTypeTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into ? values(?, ?)");
          }
        });

    queryProcessor.insert("landmark_type", 1, "Example Landmark Type");
  }
}

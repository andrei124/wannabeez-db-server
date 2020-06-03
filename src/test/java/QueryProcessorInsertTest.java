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
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement(
                    "insert into Player" + " (\"email\", \"password\") " + " values(?, ?)");
          }
        });

    queryProcessor.insertIntoPlayer("email", "password");
  }

  @Test
  public void insertIntoGalleryTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement(
                    "insert into Gallery"
                        + " (\"ts\", \"player_id\", \"url\") "
                        + " values(?, ?, ?)");
          }
        });

    queryProcessor.insertIntoGallery(exampleTS, 2, "Description");
  }

  @Test
  public void insertIntoPlayerStatsTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into Player_Stats values(?, ?, ?)");
          }
        });

    queryProcessor.insertIntoPlayerStats(12, 102, 1236);
  }

  @Test
  public void insertIntoLocationTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into Location values(?, ?)");
          }
        });

    queryProcessor.insertIntoLocation(1, examplePG);
  }

  @Test
  public void insertIntoLandmarkTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement(
                    "insert into Landmark"
                        + " (\"location\", \"type\", \"description\") "
                        + " values(?, ?, ?)");
          }
        });

    queryProcessor.insertIntoLandmark(examplePG, 6, "Example Landmark");
  }

  @Test
  public void insertIntoLandmarkTypeTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into Landmark_Type" + " (\"name\") " + " values(?)");
          }
        });

    queryProcessor.insertIntoLandmarkType("Example Landmark Type");
  }
}

import org.junit.Rule;
import org.junit.Test;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.postgis.PGgeometry;
import org.postgis.Point;
import java.sql.Connection;
import java.sql.ResultSet;
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

    queryProcessor.addNewPlayer("email", "password");
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
                        + " values(?, ?, ?) returning id");
          }
        });

    queryProcessor.addNewImageMetaData(exampleTS, 2, "Description");
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

    queryProcessor.addPlayerStats(12, 102, 1236);
  }

  @Test
  public void insertIntoLocationTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into Location values(?, ?)");
          }
        });

    queryProcessor.addNewLocation(1, examplePG);
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

    queryProcessor.addNewLandmark(examplePG, 6, "Example Landmark");
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

    queryProcessor.addNewLandmarkType("Example Landmark Type");
  }

  @Test
  public void insertIntoQuestTypeTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into Quest_Type" + " (\"name\") " + " values(?)");
          }
        });

    queryProcessor.addNewQuestType("Example Quest Type");
  }

  @Test
  public void insertIntoQuestTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement(
                    "insert into Quest"
                        + " (\"type\", \"name\", \"description\") "
                        + " values(?, ?, ?)");
          }
        });

    queryProcessor.addNewQuest(26, "Example Quest Name", "Sample description");
  }

  @Test
  public void insertIntoQuestLocationTableParsedCorrectly() throws SQLException {

    context.checking(
        new Expectations() {
          {
            exactly(1)
                .of(mockJDBCconnection)
                .prepareStatement("insert into Quest_Location" + " values(?, ?)");
          }
        });

    queryProcessor.addNewQuestLocation(26, examplePG);
  }
}

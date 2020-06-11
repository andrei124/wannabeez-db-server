import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

public class Server {

  private final QueryProcessor queryProcessor;
  private final HttpServer httpServer;

  public Server(int port, QueryProcessor queryProcessor) throws IOException {
    this.queryProcessor = queryProcessor;
    this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    /* QueryProcessor contexts */
    HttpContext insertContext = this.httpServer.createContext("/insert");
    insertContext.setHandler(this::handleInsert);
    HttpContext selectContext = this.httpServer.createContext("/select");
    selectContext.setHandler(this::handleSelect);
    HttpContext updateContext = this.httpServer.createContext("/update");
    updateContext.setHandler(this::handleUpdate);
    HttpContext deleteContext = this.httpServer.createContext("/delete");
    deleteContext.setHandler(this::handleDelete);
    HttpContext geoSelectContext = this.httpServer.createContext("/geoSelect");
    geoSelectContext.setHandler(this::handleGeoSelect);
  }

  /** Method for server boot */
  public void start() {
    this.queryProcessor.connect();
    this.httpServer.start();
    System.out.println("Server started");
  }

  /**
   * Handler for Insert into DB Http Request
   *
   * @param exchange -- HttpExchange to be processed
   * @throws IOException
   */
  private void handleInsert(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String method = requestURI.getPath().replace("/insert/", "");
    Map<String, String> params = DBInterfaceHelpers.parseQuery(requestURI.getQuery());

    String response = DBInterfaceHelpers.METHOD_NOT_FOUND;
    try {
      // check which method is being used
      switch (method) {
        case "gallery":
          {
            System.out.println("gallery insertion");
            Integer imageId =
                this.queryProcessor.addNewImageMetaData(
                    Timestamp.valueOf(DBInterfaceHelpers.safeMapLookup(params, "timestamp")),
                    Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "player")),
                    DBInterfaceHelpers.safeMapLookup(params, "url"));
            response = imageId.toString();
            break;
          }
        case "player":
          {
            System.out.println("player insertion");
            this.queryProcessor.addNewPlayer(
                DBInterfaceHelpers.safeMapLookup(params, "email"),
                DBInterfaceHelpers.safeMapLookup(params, "password"));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "player_stats":
          {
            System.out.println("player_stats insertion");
            this.queryProcessor.addPlayerStats(
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "player")),
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "xp")),
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "cash")));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "location":
          {
            System.out.println("location insertion");
            this.queryProcessor.addNewLocation(
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "image_id")),
                Float.parseFloat(DBInterfaceHelpers.safeMapLookup(params, "lon")),
                Float.parseFloat(DBInterfaceHelpers.safeMapLookup(params, "lat")));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "landmark":
          {
            System.out.println("landmark insertion");
            this.queryProcessor.addNewLandmark(
                Float.parseFloat(DBInterfaceHelpers.safeMapLookup(params, "lon")),
                Float.parseFloat(DBInterfaceHelpers.safeMapLookup(params, "lat")),
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "type")),
                DBInterfaceHelpers.safeMapLookup(params, "description"));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "landmark_type":
          {
            System.out.println("landmark_type insertion");
            this.queryProcessor.addNewLandmarkType(
                DBInterfaceHelpers.safeMapLookup(params, "name"));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "quest":
          {
            System.out.println("quest insertion");
            this.queryProcessor.addNewQuest(
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "type")),
                DBInterfaceHelpers.safeMapLookup(params, "name"),
                DBInterfaceHelpers.safeMapLookup(params, "description"));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "quest_location":
          {
            System.out.println("quest_location insertion");
            this.queryProcessor.addNewQuestLocation(
                Integer.parseInt(DBInterfaceHelpers.safeMapLookup(params, "quest_id")),
                Float.parseFloat(DBInterfaceHelpers.safeMapLookup(params, "lon")),
                Float.parseFloat(DBInterfaceHelpers.safeMapLookup(params, "lat")));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
        case "quest_type":
          {
            System.out.println("quest_type insertion");
            this.queryProcessor.addNewQuestType(DBInterfaceHelpers.safeMapLookup(params, "name"));
            response = DBInterfaceHelpers.SUCCESS;
            break;
          }
      }
    } catch (KeyNotFoundException | IllegalArgumentException e) {
      // catch missing params
      response = DBInterfaceHelpers.BAD_PARAMS;
    } catch (SQLException e) {
      response = DBInterfaceHelpers.DATABASE_ERROR;
    }
    // send response
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }

  /**
   * Handler for Select Query from DB Http Request
   *
   * @param exchange -- HttpExchange to be processed
   * @throws IOException
   */
  private void handleSelect(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String rawColumns = requestURI.getPath().replace("/select/", "");
    Map<String, String> params = DBInterfaceHelpers.parseQuery(requestURI.getQuery());

    String response = DBInterfaceHelpers.METHOD_NOT_FOUND;
    try {
      String columnsToBeQueried = rawColumns.replace("&", ",");
      String table = DBInterfaceHelpers.safeMapLookup(params, "from");
      String indexColumn = null;
      String tableToJoin = null;
      String lhsAttr = null;
      String rhsAttr = null;
      boolean validQuery = true;
      boolean joinPresent = false;

      SelectQueryBuilder queryBuilder = queryProcessor.select(columnsToBeQueried).from(table);

      // Check for WHERE clause
      try {
        indexColumn = DBInterfaceHelpers.safeMapLookup(params, "where");
        System.out.println("Select query with WHERE clause requested");
      } catch (KeyNotFoundException e) {
        System.out.println("Simple Select query requested");
      }
      if (indexColumn != null) {
        queryBuilder =
            (SelectQueryBuilder)
                DBInterfaceHelpers.getWhereClause(queryBuilder, params, indexColumn);
      }

      // Check for JOIN clause
      try {
        tableToJoin = DBInterfaceHelpers.safeMapLookup(params, "join");
        joinPresent = true;
        // Cannot have both JOIN and WHERE clause
        if (indexColumn != null) {
          validQuery = false;
          response = DBInterfaceHelpers.BAD_PARAMS;
          System.out.println("Query badly written...Cannot have both JOIN and WHERE");
        }
        System.out.println("Select query with JOIN clause requested");
      } catch (KeyNotFoundException e) {
        System.out.println("No Join query");
      }
      if (joinPresent && validQuery) {
        queryBuilder = queryBuilder.join(tableToJoin);
        try {
          lhsAttr = DBInterfaceHelpers.safeMapLookup(params, "on");
          rhsAttr = DBInterfaceHelpers.safeMapLookup(params, "equals");
          queryBuilder = queryBuilder.on(lhsAttr).equals(rhsAttr);
        } catch (KeyNotFoundException e) {
          System.out.println("Syntax error in Join query");
          response = DBInterfaceHelpers.BAD_PARAMS;
          validQuery = false;
        }
      }

      if (validQuery) {
        // Print the SQL Query
        System.out.println(queryBuilder.getSQLStatement().toString());
        ResultSet rs = queryBuilder.executeSelect();
        response = DBInterfaceHelpers.SUCCESS;
        response = response + "\n" + DBInterfaceHelpers.getJSONfromResultSet(rs);
      }
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = DBInterfaceHelpers.BAD_PARAMS;
    } catch (SQLException e) {
      response = DBInterfaceHelpers.DATABASE_ERROR;
    } catch (JSONException e) {
      response += "\nCould not retrieve JSON Object...Exception caught...";
    }
    // send response
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }

  /**
   * Handler for Update a record into DB Http Request
   *
   * @param exchange -- HttpExchange to be processed
   * @throws IOException
   */
  private void handleUpdate(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String table = requestURI.getPath().replace("/update/", "");
    Map<String, String> params = DBInterfaceHelpers.parseQuery(requestURI.getQuery());

    String response = DBInterfaceHelpers.METHOD_NOT_FOUND;
    try {
      System.out.println(table + " update");
      String column = DBInterfaceHelpers.safeMapLookup(params, "set");
      String indexColumn = DBInterfaceHelpers.safeMapLookup(params, "where");

      UpdateStatementBuilder builder = queryProcessor.update(table);
      ;
      builder = DBInterfaceHelpers.getUpdateSetTo(builder, params, column);
      builder =
          (UpdateStatementBuilder) DBInterfaceHelpers.getWhereClause(builder, params, indexColumn);

      System.out.println(builder.getSQLStatement().toString());
      builder.execute();
      response = DBInterfaceHelpers.SUCCESS;
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = DBInterfaceHelpers.BAD_PARAMS;
    } catch (SQLException e) {
      response = DBInterfaceHelpers.DATABASE_ERROR;
    }
    // send response
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }

  /**
   * Handler for Delete from DB Http Request
   *
   * @param exchange -- HttpExchange to be processed
   * @throws IOException
   */
  private void handleDelete(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    requestURI.getPath().replace("/delete/", "");
    Map<String, String> params = DBInterfaceHelpers.parseQuery(requestURI.getQuery());

    String response = DBInterfaceHelpers.METHOD_NOT_FOUND;
    try {
      String table = DBInterfaceHelpers.safeMapLookup(params, "from");
      String indexColumn = null;

      try {
        indexColumn = DBInterfaceHelpers.safeMapLookup(params, "where");
        System.out.println("Delete statement with WHERE clause invoked");
      } catch (KeyNotFoundException e) {
        System.out.println("Simple Delete statement invoked");
      }

      DeleteStatementBuilder builder = queryProcessor.delete().from(table);

      if (indexColumn != null) {
        builder =
            (DeleteStatementBuilder)
                DBInterfaceHelpers.getWhereClause(builder, params, indexColumn);
      }
      System.out.println(builder.getSQLStatement().toString());
      builder.execute();
      response = DBInterfaceHelpers.SUCCESS;
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = DBInterfaceHelpers.BAD_PARAMS;
    } catch (SQLException e) {
      response = DBInterfaceHelpers.DATABASE_ERROR;
    }
    // send response
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }

  /**
   * Handler for Geolocation Query from DB Http Request
   *
   * @param exchange -- HttpExchange to be processed
   * @throws IOException
   */
  private void handleGeoSelect(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String method = requestURI.getPath().replace("/geoSelect/", "");
    Map<String, String> params = DBInterfaceHelpers.parseQuery(requestURI.getQuery());

    String response = DBInterfaceHelpers.METHOD_NOT_FOUND;
    try {

      // Geolocation query must contain location parameter, throw exception otherwise
      Double latitude = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "lat"));
      Double longitude = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "lon"));
      Double radius = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "rad"));

      ResultSet rs = null;
      SelectQueryBuilder queryBuilder;

      switch (method) {
        case "landmark":
          System.out.println("try to spawn landmarks");
          queryBuilder =
              queryProcessor
                  .select("*")
                  .from("landmark")
                  .withinRadiusOf(latitude, longitude, radius, "landmark", "location");
          System.out.println(queryBuilder.getSQLStatement().toString());
          rs = queryBuilder.executeSelect();
          break;
        case "quest":
          System.out.println("try to spawn quests");
          queryBuilder =
              queryProcessor
                  .select("*")
                  .from("quest")
                  .withinRadiusOf(latitude, longitude, radius, "quest_location", "location");
          System.out.println(queryBuilder.getSQLStatement().toString());
          rs = queryBuilder.executeSelect();
          break;
      }
      if (rs != null) {
        response = DBInterfaceHelpers.SUCCESS;
        response = response + "\n" + DBInterfaceHelpers.getJSONfromResultSet(rs);
      }
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = DBInterfaceHelpers.BAD_PARAMS;
    } catch (SQLException e) {
      response = DBInterfaceHelpers.DATABASE_ERROR;
    } catch (JSONException e) {
      response += "\nCould not retrieve JSON Object...Exception caught...";
    }
    // send response
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }
}

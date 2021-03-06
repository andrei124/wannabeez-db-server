import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgis.Point;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
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
    HttpContext registerContext = this.httpServer.createContext("/register");
    registerContext.setHandler(this::handleRegister);
    HttpContext authContext = this.httpServer.createContext("/auth");
    authContext.setHandler(this::handleAuth);
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

      ResultSet rs = null;
      SelectQueryBuilder queryBuilder;
      Double latitude;
      Double longitude;
      Double radius;

      switch (method) {
        case "landmark":
          latitude = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "lat"));
          longitude = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "lon"));
          radius = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "rad"));
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
          latitude = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "lat"));
          longitude = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "lon"));
          radius = Double.parseDouble(DBInterfaceHelpers.safeMapLookup(params, "rad"));
          System.out.println("try to spawn quests");
          queryBuilder =
              queryProcessor
                  .select("*")
                  .from("quest")
                  .withinRadiusOf(latitude, longitude, radius, "quest_location", "location");
          System.out.println(queryBuilder.getSQLStatement().toString());
          rs = queryBuilder.executeSelect();
          break;
        case "location":
          System.out.println("find image locations");
          /* Sample Poly JSON:
          [{"lat":52.43042541356032,"lng":-4.966549702756713},{"lat":46.741212852594806,"lng":-9.119381734006712},
          {"lat":44.73252738390248,"lng":16.588626078493288},{"lat":56.77788943094397,"lng":7.118411234743287},
          {"lat":57.37500227162194,"lng":-21.709713765256712}]
           */
          // Parse poly from json
          JSONArray vertsJSON = new JSONArray(DBInterfaceHelpers.safeMapLookup(params, "poly"));
          List<Point> vertices = new ArrayList<>();
          // Check there are enough verts
          if (vertsJSON.length() < 3)
            throw new KeyNotFoundException();
          // Populate verts
          for (int i = 0; i < vertsJSON.length(); i++) {
            JSONObject v = new JSONObject(vertsJSON.get(i).toString());
            vertices.add(new Point(v.getDouble("lng"), v.getDouble("lat")));
          }
          // Send
          SelectQueryBuilder innerSubQueryBuilder =
              queryProcessor
              .select("*")
              .from("location")
              .withinPoly(vertices, "location", "location");

          SelectQueryBuilder outerSubQueryBuilder =
              queryProcessor
                  .select("t1.id, t1.location, url")
                  .from("(" + innerSubQueryBuilder.getSQLStatement().toString() + ") t1")
                  .innerJoin("gallery")
                  .on("gallery.id = t1.id");

          queryBuilder =
              queryProcessor
                  .select("id, " +
                      "ST_X(ST_Centroid(ST_Transform(location, 4326))) AS long, " +
                      "ST_Y(ST_Centroid(ST_Transform(location, 4326))) AS lat, " +
                      "url")
                  .from("(" + outerSubQueryBuilder.getSQLStatement().toString() + ") t2");

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

  private void handleRegister(HttpExchange exchange) throws IOException {
    byte[] jsonCredentialsAsBytes = exchange.getRequestBody().readAllBytes();
    String credentials = new String(jsonCredentialsAsBytes, StandardCharsets.UTF_8);

    JSONObject credentialsAsJSON = new JSONObject(credentials);
    String email = credentialsAsJSON.getString("email");
    String password = credentialsAsJSON.getString("password");

    String response = DBInterfaceHelpers.LOGIN_SUCCESSFUL;
    try {
      Integer playerId = queryProcessor.addNewPlayer(email, password);
      queryProcessor.addPlayerStats(
          playerId, DBInterfaceHelpers.DEFAULT_XP, DBInterfaceHelpers.DEFAULT_CASH);
    } catch (SQLException e) {
      System.out.println("Fail...a player with this email already exists");
      response = DBInterfaceHelpers.WRONG_CREDENTIALS;
    }
    System.out.println("Register succsesful");
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }

  private void handleAuth(HttpExchange exchange) throws IOException {
    byte[] jsonCredentialsAsBytes = exchange.getRequestBody().readAllBytes();
    String credentials = new String(jsonCredentialsAsBytes, StandardCharsets.UTF_8);
    String response;

    JSONObject credentialsAsJSON = new JSONObject(credentials);
    String email = credentialsAsJSON.getString("email");
    String password = credentialsAsJSON.getString("password");

    try {
      ResultSet rs =
          queryProcessor.select("*").from("Player").where("email").is(email).executeSelect();
      if (!rs.next()) {
        response = DBInterfaceHelpers.WRONG_CREDENTIALS;
      } else {
        if (!rs.getString("password").equals(password)) {
          response = DBInterfaceHelpers.WRONG_CREDENTIALS;
        } else {
          response = DBInterfaceHelpers.LOGIN_SUCCESSFUL;
        }
      }
    } catch (SQLException e) {
      response = DBInterfaceHelpers.DB_ERROR;
    }
    System.out.println("Auth successful");
    DBInterfaceHelpers.sendResponseBackToClient(exchange, response);
  }
}

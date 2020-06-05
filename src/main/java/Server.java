import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgis.PGgeometry;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Server {

  private static final String METHOD_NOT_FOUND = "method not found";
  private static final String SUCCESS = "success";
  private static final String BAD_PARAMS = "bad parameters";
  private static final String DATABASE_ERROR = "database error";

  private final QueryProcessor queryProcessor;
  private final HttpServer httpServer;

  public Server(int port, QueryProcessor queryProcessor)
      throws IOException {
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
  }

  public static Map<String, String> parseQuery(String url) throws UnsupportedEncodingException {
    Map<String, String> queryPairs = new HashMap<>();
    url = URLDecoder.decode(url, "UTF-8");
    if (url != null) {
      String[] pairs = url.split("&");
      for (String pair : pairs) {
        int idx = pair.indexOf("=");
        queryPairs.put(pair.substring(0, idx), pair.substring(idx + 1));
      }
    }
    return queryPairs;
  }

  public static String safeMapLookup(Map<String, String> map, String key)
      throws KeyNotFoundException {
    if (map.get(key) == null) throw new KeyNotFoundException();
    return map.get(key);
  }

  public void start() {
    this.queryProcessor.connect();
    this.httpServer.start();
    System.out.println("Server started");
  }

  private void handleInsert(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String method = requestURI.getPath().replace("/insert/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      // check which method is being used
      switch (method) {
        case "gallery":
          {
            System.out.println("gallery insertion");
            this.queryProcessor.addNewImageMetaData(
                Timestamp.valueOf(safeMapLookup(params, "timestamp")),
                Integer.parseInt(safeMapLookup(params, "player")),
                safeMapLookup(params, "url"));
            response = SUCCESS;
            break;
          }
        case "player":
          {
            System.out.println("player insertion");
            this.queryProcessor.addNewPlayer(
                safeMapLookup(params, "email"), safeMapLookup(params, "password"));
            response = SUCCESS;
            break;
          }
        case "player_stats":
          {
            System.out.println("player_stats insertion");
            this.queryProcessor.addPlayerStats(
                Integer.parseInt(safeMapLookup(params, "player")),
                Integer.parseInt(safeMapLookup(params, "xp")),
                Integer.parseInt(safeMapLookup(params, "cash")));
            response = SUCCESS;
            break;
          }
        case "location":
          {
            System.out.println("location insertion");
            this.queryProcessor.addNewLocation(
                Integer.parseInt(safeMapLookup(params, "image_id")),
                new PGgeometry(safeMapLookup(params, "location")));
            response = SUCCESS;
            break;
          }
        case "landmark":
          {
            System.out.println("landmark insertion");
            this.queryProcessor.addNewLandmark(
                new PGgeometry(safeMapLookup(params, "location")),
                Integer.parseInt(safeMapLookup(params, "type")),
                safeMapLookup(params, "description"));
            response = SUCCESS;
            break;
          }
        case "landmark_type":
          {
            System.out.println("landmark_type insertion");
            this.queryProcessor.addNewLandmarkType(safeMapLookup(params, "name"));
            response = SUCCESS;
            break;
          }
      }
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = BAD_PARAMS;
    } catch (NumberFormatException e) {
      // catch missing params
      response = BAD_PARAMS;
    } catch (IllegalArgumentException e) {
      response = BAD_PARAMS;
    } catch (SQLException e) {
      response = DATABASE_ERROR;
    }

    // send response
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private void handleSelect(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String rawColumns = requestURI.getPath().replace("/select/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      String columnsToBeQueried = rawColumns.replace("&", ",");
      String table = safeMapLookup(params, "from");
      String indexColumn = null;

      try {
        indexColumn = safeMapLookup(params, "where");
        System.out.println("Select query with WHERE clause requested");
      } catch (KeyNotFoundException e) {
        System.out.println("Simple Select query requested");
      }
      SelectQueryBuilder queryBuilder = queryProcessor.select(columnsToBeQueried).from(table);
      if (indexColumn != null) {
        queryBuilder = (SelectQueryBuilder) getWhereClause(queryBuilder, params, indexColumn);
      }

      System.out.println(queryBuilder.getSQLStatement().toString());
      ResultSet rs = queryBuilder.executeSelect();
      response = SUCCESS;
      response = response + "\n" + getJSONfromResultSet(rs);
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = BAD_PARAMS;
    } catch (SQLException e) {
      response = DATABASE_ERROR;
    } catch (JSONException e) {
      response += "\nCould not retrieve JSON Object...Exception caught...";
    }

    // send response
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private String getJSONfromResultSet(ResultSet rs) throws SQLException, JSONException {
    List<JSONObject> jsonObjectList = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();
    ResultSetMetaData rsMetaData = rs.getMetaData();
    int columnsSelected = rsMetaData.getColumnCount();

    for(int i = 0; i < columnsSelected; i++) {
      columnNames.add(i, rsMetaData.getColumnName(i + 1));
    }

    while (rs.next()) {
      JSONObject jsonObject = new JSONObject();
      for (int i = 0; i < columnsSelected; i++) {
        String key = columnNames.get(i);
        String value = rs.getString(i + 1);
        jsonObject.put(key, value);
      }
      jsonObjectList.add(jsonObject);
    }
    return jsonObjectList.toString();
  }

  private void handleUpdate(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String table = requestURI.getPath().replace("/update/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      System.out.println(table + " update");
      String column = safeMapLookup(params, "set");
      String indexColumn = safeMapLookup(params, "where");

      UpdateStatementBuilder builder = queryProcessor.update(table);
      ;
      builder = getUpdateSetTo(builder, params, column);
      builder = (UpdateStatementBuilder) getWhereClause(builder, params, indexColumn);

      System.out.println(builder.getSQLStatement().toString());

      builder.execute();
      response = SUCCESS;
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = BAD_PARAMS;
    } catch (SQLException e) {
      response = DATABASE_ERROR;
    }
    // send response
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private void handleDelete(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    requestURI.getPath().replace("/delete/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      String table = safeMapLookup(params, "from");
      String indexColumn = null;

      try {
        indexColumn = safeMapLookup(params, "where");
        System.out.println("Delete statement with WHERE clause invoked");
      } catch (KeyNotFoundException e) {
        System.out.println("Simple Delete statement invoked");
      }

      DeleteStatementBuilder builder = queryProcessor.delete().from(table);

      if (indexColumn != null) {
        builder = (DeleteStatementBuilder) getWhereClause(builder, params, indexColumn);
      }
      System.out.println(builder.getSQLStatement().toString());
      builder.execute();
      response = SUCCESS;
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = BAD_PARAMS;
    } catch (SQLException e) {
      response = DATABASE_ERROR;
    }
    // send response
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private UpdateStatementBuilder getUpdateSetTo(
      UpdateStatementBuilder builder, Map<String, String> params, String column)
      throws KeyNotFoundException, SQLException {
    switch (column) {
      case "player_id":
      case "id":
      case "type":
      case "xp":
      case "cash":
        {
          builder = builder.set(column).to(Integer.parseInt(safeMapLookup(params, "to")));
          break;
        }
      case "ts":
        {
          builder = builder.set(column).to(Timestamp.valueOf(safeMapLookup(params, "to")));
          break;
        }
      case "email":
      case "password":
      case "url":
      case "description":
      case "name":
        {
          builder = builder.set(column).to(safeMapLookup(params, "to"));
          break;
        }
      case "location":
        {
          builder = builder.set(column).to(new PGgeometry(safeMapLookup(params, "to")));
          break;
        }
      default:
        System.out.println("DEFAULT CASE HAS BEEN REACHED");
    }
    return builder;
  }

  private WhereClauseBuilder getWhereClause(
      WhereClauseBuilder builder, Map<String, String> params, String column)
      throws KeyNotFoundException, SQLException {
    switch (column) {
      case "player_id":
      case "id":
      case "type":
      case "xp":
      case "cash":
        {
          builder = builder.where(column).is(Integer.parseInt(safeMapLookup(params, "is")));
          break;
        }
      case "ts":
        {
          builder = builder.where(column).is(Timestamp.valueOf(safeMapLookup(params, "is")));
          break;
        }
      case "email":
      case "password":
      case "url":
      case "description":
      case "name":
        {
          builder = builder.where(column).is(safeMapLookup(params, "is"));
          break;
        }
      case "location":
        {
          builder = builder.where(column).is(new PGgeometry(safeMapLookup(params, "is")));
          break;
        }
      default:
        System.out.println("DEFAULT CASE HAS BEEN REACHED");
    }
    return builder;
  }
}

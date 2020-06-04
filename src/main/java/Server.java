import com.sun.net.httpserver.*;
import org.postgis.PGgeometry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class Server {

  private static final String METHOD_NOT_FOUND = "method not found";
  private static final String SUCCESS = "success";
  private static final String BAD_PARAMS = "bad parameters";
  private static final String DATABASE_ERROR = "database error";

  private final QueryProcessor queryProcessor;
  private final HttpServer httpServer;

  public Server(int port, QueryProcessor queryProcessor) throws IOException {
    this.queryProcessor = queryProcessor;
    this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    HttpContext insertContext = this.httpServer.createContext("/insert");
    insertContext.setHandler(this::handleInsert);
    HttpContext selectContext = this.httpServer.createContext("/select");
    selectContext.setHandler(this::handleSelect);
    HttpContext updateContext = this.httpServer.createContext("/update");
    updateContext.setHandler(this::handleUpdate);
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
    String method = requestURI.getPath().replace("/select/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      // check which method is being used
      switch (method) {
        case "gallery":
          {
            System.out.println("gallery select");
            safeMapLookup(params, "asdfasdf");
            response = SUCCESS;
            break;
          }
      }
    } catch (KeyNotFoundException e) {
      // catch missing params
      response = BAD_PARAMS;
    }

    // send response
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
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
      builder = getUpdateWhereClause(builder, params, indexColumn);

      System.out.println(builder.getSqlUpdateStatement().toString());

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

  private UpdateStatementBuilder getUpdateWhereClause(
      UpdateStatementBuilder builder, Map<String, String> params, String column)
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

  public static Map<String, String> parseQuery(String url) {
    Map<String, String> queryPairs = new HashMap<>();
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
}

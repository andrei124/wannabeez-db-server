import com.sun.net.httpserver.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.SQLException;
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
    insertContext.setHandler(Server::handleInsert);
    HttpContext selectContext = this.httpServer.createContext("/select");
    selectContext.setHandler(Server::handleSelect);
    HttpContext updateContext = this.httpServer.createContext("/update");
    updateContext.setHandler(Server::handleUpdate);
  }

  public void start() {
    this.queryProcessor.connect();
    this.httpServer.start();
    System.out.println("Server started");
  }

  private static void handleInsert(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String method = requestURI.getPath().replace("/insert/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      // check which method is being used
      switch (method) {
        case "gallery":
          System.out.println("gallery insertion");
          //TODO: QUERYPROCESSOR GALLERY SHIT
          // SOMETHING LIKE THIS : this.queryProcessor.insert("gallery", .....);
          safeMapLookup(params, "asdfasdf");
          response = SUCCESS;
          break;
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

  private static void handleSelect(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String method = requestURI.getPath().replace("/insert/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      // check which method is being used
      switch (method) {
        case "gallery":
          System.out.println("gallery select");
          safeMapLookup(params, "asdfasdf");
          response = SUCCESS;
          break;
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

  private static void handleUpdate(HttpExchange exchange) throws IOException {
    // extract info from request
    URI requestURI = exchange.getRequestURI();
    String method = requestURI.getPath().replace("/insert/", "");
    Map<String, String> params = parseQuery(requestURI.getQuery());

    String response = METHOD_NOT_FOUND;
    try {
      // check which method is being used
      switch (method) {
        case "gallery":
          System.out.println("gallery update");
          safeMapLookup(params, "asdfasdf");
          response = SUCCESS;
          break;
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

  public static String safeMapLookup(Map<String, String> map, String key) throws KeyNotFoundException{
    if (map.get(key) == null)
      throw new KeyNotFoundException();
    return map.get(key);
  }

}

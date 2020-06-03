import com.sun.net.httpserver.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.SQLException;

public class Server {

  private final QueryProcessor queryProcessor;
  private final HttpServer httpServer;

  public Server(int port, QueryProcessor queryProcessor) throws IOException {
    this.queryProcessor = queryProcessor;
    this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    HttpContext insertContext = this.httpServer.createContext("/insert");
    insertContext.setHandler(Server::handleInsert);
    HttpContext selectContext = this.httpServer.createContext("/select");
    selectContext.setHandler(Server::handleSelect);
    HttpContext updateContext = this.httpServer.createContext("/select");
    updateContext.setHandler(Server::handleUpdate);
  }

  public void start() {
    this.httpServer.start();
    System.out.println("Server started");
  }

  private static void handleInsert(HttpExchange exchange) throws IOException {
    URI requestURI = exchange.getRequestURI();
    printRequestInfo(exchange);
    String response = "This is the response at " + requestURI;
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private static void handleSelect(HttpExchange exchange) throws IOException {
    URI requestURI = exchange.getRequestURI();
    printRequestInfo(exchange);
    String response = "This is the response at " + requestURI;
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private static void handleUpdate(HttpExchange exchange) throws IOException {
    URI requestURI = exchange.getRequestURI();
    printRequestInfo(exchange);
    String response = "This is the response at " + requestURI;
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  private static void printRequestInfo(HttpExchange exchange) {
    System.out.println("-- headers --");
    Headers requestHeaders = exchange.getRequestHeaders();
    requestHeaders.entrySet().forEach(System.out::println);

    System.out.println("-- principle --");
    HttpPrincipal principal = exchange.getPrincipal();
    System.out.println(principal);

    System.out.println("-- HTTP method --");
    String requestMethod = exchange.getRequestMethod();
    System.out.println(requestMethod);

    System.out.println("-- query --");
    URI requestURI = exchange.getRequestURI();
    String query = requestURI.getQuery();
    System.out.println(query);
  }

}

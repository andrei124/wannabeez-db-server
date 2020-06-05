import com.sun.net.httpserver.HttpExchange;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgis.PGgeometry;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.*;
import java.util.*;

public class DBInterfaceHelpers {

  public static final String METHOD_NOT_FOUND = "method not found";
  public static final String SUCCESS = "success";
  public static final String BAD_PARAMS = "bad parameters";
  public static final String DATABASE_ERROR = "database error";

  /**
   * Helper method for executing an SQL Prepared Statement
   *
   * @param stmt -- prepared statement to be executed
   * @throws SQLException
   */
  public static void executeSQLStatement(PreparedStatement stmt) throws SQLException {
    stmt.executeUpdate();
  }

  /**
   * Helper method for retrieving the result of an SQL query *
   *
   * @param stmt - statement to retrieve the result for
   * @return
   * @throws SQLException
   */
  public static ResultSet getResultSet(PreparedStatement stmt) throws SQLException {
    return stmt.executeQuery();
  }

  /**
   * Helper method for getting the columns to be queried into the SELECT statement *
   *
   * @param arguments - list of column names to be selected
   * @return
   */
  public static String getColumnsToBeQueried(String... arguments) {
    List<String> args = Arrays.asList(arguments);
    int len = args.size();
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < len; i++) {
      if (i != len - 1) {
        sb.append(args.get(i)).append(",");
      } else {
        sb.append(args.get(i));
      }
    }
    return sb.toString();
  }

  /**
   * Method for parsing the URL of a query sent via an HTTP Request
   *
   * @param url -- URL of query to be parsed
   * @return -- Map<Key, Value> e.g. ("from", "player")
   * @throws UnsupportedEncodingException
   */
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

  /**
   * Method for formatting the ResultSet of a Select query into JSON format
   *
   * @param rs -- ResultSet of the query invoked
   * @return -- String - representing a list of JSON objects
   * @throws SQLException
   * @throws JSONException
   */
  public static String getJSONfromResultSet(ResultSet rs) throws SQLException, JSONException {
    List<JSONObject> jsonObjectList = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();
    ResultSetMetaData rsMetaData = rs.getMetaData();
    int columnsSelected = rsMetaData.getColumnCount();

    for (int i = 0; i < columnsSelected; i++) {
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

  /**
   * Method for sending response of a SQL Query back to the Client
   *
   * @param exchange -- the HttpExchange to be processed
   * @param response -- the response sent back to the client
   * @throws IOException
   */
  public static void sendResponseBackToClient(HttpExchange exchange, String response)
      throws IOException {
    exchange.sendResponseHeaders(200, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }

  /**
   * Method for retrieving the value corresponding to a key in a map
   *
   * @param map -- Map to be looked up
   * @param key -- key to be looked up
   * @return -- Value corresponding to looked up key
   * @throws KeyNotFoundException
   */
  public static String safeMapLookup(Map<String, String> map, String key)
      throws KeyNotFoundException {
    if (map.get(key) == null) throw new KeyNotFoundException();
    return map.get(key);
  }

  /**
   * Method for setting up the basic parameters SET and TO of an Update statement
   *
   * @param builder -- Update Statement Builder to be processed
   * @param params -- map of pairs (key, value) representing parameters of the query
   * @param column -- name of column to be set
   * @return
   * @throws KeyNotFoundException
   * @throws SQLException
   */
  public static UpdateStatementBuilder getUpdateSetTo(
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

  /**
   * Method for setting up the basic parameters WHERE and IS of a SQL statement with a WHERE clause
   *
   * @param builder -- Where Clause Builder to be processed
   * @param params -- map of pairs (key, value) representing parameters of the query
   * @param column -- name of column to be set
   * @return
   * @throws KeyNotFoundException
   * @throws SQLException
   */
  public static WhereClauseBuilder getWhereClause(
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

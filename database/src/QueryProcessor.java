import java.sql.*;


public class QueryProcessor {

    public QueryProcessor() {
        try {
            registerDriver();
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC Driver could not be registered");
            System.exit(1);
            e.printStackTrace();
        }
    }

    private void registerDriver() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
    }

}


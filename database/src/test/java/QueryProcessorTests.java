import org.junit.Rule;
import org.junit.Test;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;

import java.sql.Connection;
import java.sql.ResultSet;

public class QueryProcessorTests {

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    Connection mockJDBCconnection = context.mock(Connection.class);
    ResultSet mockResultSet = context.mock(ResultSet.class);

    QueryProcessor queryProcessor = new QueryProcessor(mockJDBCconnection);

}

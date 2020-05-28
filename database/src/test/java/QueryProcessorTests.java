import org.junit.Rule;
import org.junit.Test;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryProcessorTests {

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    Connection mockJDBCconnection = context.mock(Connection.class);
    ResultSet mockResultSet = context.mock(ResultSet.class);

    QueryProcessor queryProcessor = new QueryProcessor(mockJDBCconnection);


    @Test
    public void insertTest() throws SQLException {

        context.checking(new Expectations() {{
            exactly(1).of(mockJDBCconnection).prepareStatement("insert into PLAYER values(?, ?, ?)");
        }});

        int index = queryProcessor.insertInto("Player", "100", "email", "password");

    }

}

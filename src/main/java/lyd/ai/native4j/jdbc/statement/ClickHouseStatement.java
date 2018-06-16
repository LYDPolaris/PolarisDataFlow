package lyd.ai.native4j.jdbc.statement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lyd.ai.native4j.jdbc.ClickHouseResultSet;
import lyd.ai.native4j.jdbc.wrapper.SQLStatement;
import lyd.ai.native4j.jdbc.ClickHouseConnection;
import lyd.ai.native4j.jdbc.protocol.QueryResponse;
import lyd.ai.native4j.jdbc.stream.ValuesInputFormat;

public class ClickHouseStatement extends SQLStatement {
    private static final Pattern VALUES_REGEX = Pattern.compile("[V|v][A|a][L|l][U|u][E|e][S|s]\\s*\\(");

    private ResultSet lastResultSet;

    protected final ClickHouseConnection connection;

    public ClickHouseStatement(ClickHouseConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean execute(String query) throws SQLException {
        return executeQuery(query) != null;
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException {
        executeUpdate(query);
        return getResultSet();
    }

    @Override
    public int executeUpdate(String query) throws SQLException {
        Matcher matcher = VALUES_REGEX.matcher(query);
        if (matcher.find()) {
            lastResultSet = null;
            String insertQuery = query.substring(0, matcher.end() - 1);
            return connection.sendInsertRequest(insertQuery, new ValuesInputFormat(matcher.end() - 1, query));
        }
        QueryResponse response = connection.sendQueryRequest(query);
        lastResultSet = new ClickHouseResultSet(response.header(), response.data().iterator(), this);
        return 0;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return lastResultSet;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return lastResultSet.getMetaData();
    }
}

package com.wherobots.db.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

public class WherobotsJdbcConnection implements Connection {

    public static final Logger logger = Logger.getLogger(WherobotsJdbcConnection.class.getName());

    private static final String EXECUTE_SQL_KIND = "execute_sql";
    private static final String RETRIEVE_RESULTS_KIND = "retrieve_results";

    private static final String KIND = "kind";
    private static final String EXECUTION_ID = "execution_id";

    private record ExecuteSqlRequest(
            @JsonProperty(KIND) String kind,
            @JsonProperty(EXECUTION_ID) String executionId,
            String statement) {}

    private record RetrieveResultsRequest(
            @JsonProperty(KIND) String kind,
            @JsonProperty(EXECUTION_ID) String executionId,
            String format,
            String compression,
            String geometry) {}

    private final WherobotsSession session;
    private final ConcurrentMap<String, WherobotsStatement> queries;

    public WherobotsJdbcConnection(WherobotsSession session) {
        this.session = session;
        this.queries = new ConcurrentHashMap<>();

        Thread thread = new Thread(this::loop);
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        while (!this.isClosed()) {
            for (Frame frame : this.session) {
                try {
                    this.handle(frame.get());
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                    this.close();
                    return;
                }
            }
        }
    }

    private void handle(Map<String, Object> message) throws Exception {
        String kind = (String) message.get(KIND);
        String executionId = (String) message.get(EXECUTION_ID);
        if (StringUtils.isBlank(kind) || StringUtils.isBlank(executionId)) {
            // Invalid event.
            return;
        }

        WherobotsStatement statement = this.queries.get(executionId);
        if (statement == null) {
            logger.warning(String.format("Received event for unknown query %s.", executionId));
            return;
        }

        switch (kind) {
            case "state_updated":
                break;

            case "execution_result":
                statement.onExecutionResult(new WherobotsResultSet());
                break;

            case "error":
                break;

            default:
                logger.warning(String.format("Received unknown %s event!", kind));
        }

    }

    String execute(String sql, WherobotsStatement statement) {
        String executionId = UUID.randomUUID().toString();
        this.queries.put(executionId, statement);

        String request = JsonUtil.serialize(new ExecuteSqlRequest(
                EXECUTE_SQL_KIND,
                executionId,
                sql
        ));

        logger.info(String.format("Executing SQL query %s: %s", executionId, request));
        this.session.send(request);
        return executionId;
    }

    void retrieveResults(String executionId) {
        if (!this.queries.containsKey(executionId)) {
            return;
        }

        String request = JsonUtil.serialize(new RetrieveResultsRequest(
                RETRIEVE_RESULTS_KIND,
                executionId,
                null,
                null,
                null
        ));

        logger.info(String.format("Retrieving results from %s ...", executionId));
        this.session.send(request);
    }

    void cancel(String executionId) {
        WherobotsStatement statement = this.queries.remove(executionId);
        if (statement != null) {
            statement.close();
            logger.info(String.format("Cancelled query %s.", executionId));
        }
    }

    @Override
    public Statement createStatement() {
        return new WherobotsStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        // TODO
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stored procedures are not supported");
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public boolean getAutoCommit() {
        return false;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public void close() {
        this.session.close();
    }

    @Override
    public boolean isClosed() {
        return this.session.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("Read-only mode is not supported");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return "";
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // No-op
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return Map.of();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Stored procedures are not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO: send dummy query to validate the connection.
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return "";
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return "";
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}

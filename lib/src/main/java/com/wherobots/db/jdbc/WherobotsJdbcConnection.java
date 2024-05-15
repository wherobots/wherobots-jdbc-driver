package com.wherobots.db.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wherobots.db.jdbc.internal.Frame;
import com.wherobots.db.jdbc.internal.JsonUtil;
import com.wherobots.db.jdbc.internal.Query;
import com.wherobots.db.jdbc.internal.QueryState;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

public class WherobotsJdbcConnection implements Connection {

    public static final Logger logger = LoggerFactory.getLogger(WherobotsJdbcConnection.class);

    private static final String EXECUTE_SQL_KIND = "execute_sql";
    private static final String RETRIEVE_RESULTS_KIND = "retrieve_results";

    private static final String KIND = "kind";
    private static final String EXECUTION_ID = "execution_id";
    public static final String STATE = "state";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record ExecuteSqlRequest(
            @JsonProperty(KIND) String kind,
            @JsonProperty(EXECUTION_ID) String executionId,
            String statement) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record RetrieveResultsRequest(
            @JsonProperty(KIND) String kind,
            @JsonProperty(EXECUTION_ID) String executionId,
            DataFormat format,
            DataCompression compression,
            GeometryRepresentation geometry) {}

    private final WherobotsSession session;
    private final ConcurrentMap<String, Query> queries;
    private final Properties clientInfo;

    public WherobotsJdbcConnection(WherobotsSession session) {
        this.session = session;
        this.queries = new ConcurrentHashMap<>();
        this.clientInfo = new Properties();

        Thread thread = new Thread(this::loop);
        thread.setDaemon(true);
        thread.setName("wherobots-connection");
        thread.start();
    }

    private void loop() {
        while (!this.isClosed()) {
            Iterator<Frame> iterator = this.session.iterator();
            while (iterator.hasNext()) {
                Frame frame = iterator.next();
                try {
                    this.handle(frame.get());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    this.close();
                    return;
                }
                iterator.remove();
            }
        }
    }

    private void handle(Map<String, Object> message) throws Exception {
        logger.info("handle({})", message);
        String kind = (String) message.get(KIND);
        String executionId = (String) message.get(EXECUTION_ID);
        if (StringUtils.isBlank(kind) || StringUtils.isBlank(executionId)) {
            // Invalid event.
            return;
        }

        Query query = this.queries.get(executionId);
        if (query == null) {
            logger.warn("Received event for unknown query {}.", executionId);
            return;
        }

        switch (kind) {
            case "state_updated":
                QueryState status = QueryState.valueOf((String) message.get(STATE));
                query.setStatus(status);
                logger.info("Query {} is now {}.", executionId, status);

                switch (status) {
                    case succeeded -> this.retrieveResults(executionId);
                    // TODO: handle FAILED
                }

                break;

            case "execution_result":
                Map<String, Object> results = (Map<String, Object>) message.get("results");
                byte[] data = (byte[]) results.get("result_bytes");
                String compression = (String) results.get("compression");
                String format = (String) results.get("format");
                logger.info(
                        "Received {} bytes of {}-compressed {} results from {}.",
                        data.length, compression, format, executionId);

                BufferAllocator rootAllocator = new RootAllocator();
                ZstdCompressorInputStream stream = new ZstdCompressorInputStream(new ByteArrayInputStream(data));
                ArrowStreamReader reader = new ArrowStreamReader(stream, rootAllocator, new CommonsCompressionFactory());

                query.statement().onExecutionResult(new WherobotsResultSet(reader));
                break;

            case "error":
                query.setStatus(QueryState.failed);
                String error = (String) message.get("message");
                logger.warn("Error: {}", error);
                break;

            default:
                logger.warn("Received unknown {} event!", kind);
        }

    }

    String execute(String sql, WherobotsStatement statement) {
        String executionId = UUID.randomUUID().toString();
        this.queries.put(executionId, new Query(
                executionId,
                sql,
                statement,
                QueryState.pending));

        String request = JsonUtil.serialize(new ExecuteSqlRequest(
                EXECUTE_SQL_KIND,
                executionId,
                sql
        ));

        logger.info("Executing SQL query {}: {}", executionId, request);
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
                DataCompression.zstd,
                null
        ));

        logger.info("Retrieving results from {} ...", executionId);
        this.session.send(request);
    }

    void cancel(String executionId) {
        Query query = this.queries.remove(executionId);
        if (query != null) {
            query.statement().close();
            logger.info("Cancelled query {}.", executionId);
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
        this.clientInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return (String) this.clientInfo.get(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.clientInfo;
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

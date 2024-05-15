package com.wherobots.db.jdbc;

import com.wherobots.db.DataCompression;
import com.wherobots.db.jdbc.internal.ExecutionResult;
import com.wherobots.db.jdbc.internal.Frame;
import com.wherobots.db.jdbc.models.Event;
import com.wherobots.db.jdbc.serde.ArrowUtil;
import com.wherobots.db.jdbc.serde.JsonUtil;
import com.wherobots.db.jdbc.internal.Query;
import com.wherobots.db.jdbc.models.QueryState;
import com.wherobots.db.jdbc.models.ExecuteSqlRequest;
import com.wherobots.db.jdbc.models.RetrieveResultsRequest;
import com.wherobots.db.jdbc.session.WherobotsSession;
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
import java.sql.ResultSet;
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

    private void handle(Event event) throws Exception {
        logger.info("Handling event: {}", JsonUtil.serialize(event));
        if (event.kind == null || event.executionId == null) {
            // Invalid event.
            return;
        }

        Query query = this.queries.get(event.executionId);
        if (query == null) {
            logger.warn("Received event for unknown query {}.", event.executionId);
            return;
        }

        if (event instanceof Event.StateUpdatedEvent) {
            query.setStatus(event.state);
            logger.info("Query {} is now {}.", event.executionId, event.state);

            switch (event.state) {
                case succeeded -> this.retrieveResults(event.executionId);
                case failed -> {
                    // No-op, error event will follow.
                }
            }

            return;
        }

        if (event instanceof Event.ExecutionResultEvent ere) {
            Event.Results results = ere.results;

            logger.info(
                    "Received {} bytes of {}-compressed {} results from {}.",
                    results.resultBytes.length, results.compression, results.format, event.executionId);
            ArrowStreamReader reader = ArrowUtil.readFrom(results.resultBytes, results.compression);
            query.statement().onExecutionResult(new ExecutionResult(new WherobotsResultSet(reader), null));
            return;
        }

        if (event instanceof Event.ErrorEvent error) {
            query.statement().onExecutionResult(new ExecutionResult(null, new SQLException(error.message)));
            return;
        }

        logger.warn("Received unknown event kind: {}", event.kind);
    }

    String execute(String sql, WherobotsStatement statement) {
        String executionId = UUID.randomUUID().toString();
        this.queries.put(executionId, new Query(
                executionId,
                sql,
                statement,
                QueryState.pending));

        String request = JsonUtil.serialize(new ExecuteSqlRequest(
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
    public DatabaseMetaData getMetaData() {
        // TODO
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
    public void setCatalog(String catalog) {
        // Ignore, users must specify the catalog in the SQL query.
    }

    @Override
    public String getCatalog() {
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
        throw new SQLFeatureNotSupportedException("Cursor holdability is not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor holdability is not supported");
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
        Statement stmt = this.createStatement();
        stmt.setQueryTimeout(timeout);
        try (ResultSet result = stmt.executeQuery("SELECT 1")) {
            return result.next();
        }
    }

    @Override
    public void setClientInfo(String name, String value) {
        this.clientInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) {
        this.clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) {
        return (String) this.clientInfo.get(name);
    }

    @Override
    public Properties getClientInfo() {
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
    public void setSchema(String schema) {
        // Ignore, users must specify the schema in the SQL query.
    }

    @Override
    public String getSchema() {
        return "";
    }

    @Override
    public void abort(Executor executor) {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        // No-op
    }

    @Override
    public int getNetworkTimeout() {
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

package com.wherobots.db.jdbc;

import com.wherobots.db.DataCompression;
import com.wherobots.db.DataFormat;
import com.wherobots.db.GeometryRepresentation;
import com.wherobots.db.jdbc.internal.ExecutionResult;
import com.wherobots.db.jdbc.internal.Frame;
import com.wherobots.db.jdbc.internal.Query;
import com.wherobots.db.jdbc.models.CancelRequest;
import com.wherobots.db.jdbc.models.Event;
import com.wherobots.db.jdbc.models.ExecuteSqlRequest;
import com.wherobots.db.jdbc.models.QueryState;
import com.wherobots.db.jdbc.models.RetrieveResultsRequest;
import com.wherobots.db.jdbc.models.Store;
import com.wherobots.db.jdbc.models.StoreResult;
import com.wherobots.db.jdbc.serde.ArrowUtil;
import com.wherobots.db.jdbc.serde.JsonUtil;
import com.wherobots.db.jdbc.session.WherobotsSession;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Properties info;

    public WherobotsJdbcConnection(WherobotsSession session, Properties info) {
        this.session = session;
        this.queries = new ConcurrentHashMap<>();
        this.info = info;

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
        if (logger.isDebugEnabled()) {
            logger.info("Handling event: {}", JsonUtil.serialize(event));
        }
        if (event.kind == null || event.executionId == null) {
            // Invalid event.
            return;
        }

        Query query = this.queries.get(event.executionId);
        if (query == null) {
            logger.warn("Received event for unknown query {}.", event.executionId);
            return;
        }

        if (event instanceof Event.StateUpdatedEvent sue) {
            query.setStatus(event.state);
            logger.info("Query {} is now {}.", event.executionId, event.state);

            switch (event.state) {
                case succeeded -> {
                    if (sue.resultUri != null) {
                        // Results are stored in cloud storage, push directly to queue with store result
                        StoreResult storeResult = new StoreResult(sue.resultUri, sue.size);
                        logger.info("Query {} stored result at: {} (size: {})", event.executionId, sue.resultUri, sue.size);
                        query.statement().onExecutionResult(new ExecutionResult(null, null, storeResult));
                    } else {
                        // No store configured, retrieve results normally
                        this.retrieveResults(event.executionId);
                    }
                }
                case cancelled -> query.statement().onExecutionResult(new ExecutionResult(null, null, null));
                case failed -> {
                    // No-op, error event will follow.
                }
            }

            return;
        }

        if (event instanceof Event.ExecutionResultEvent ere) {
            Event.Results results = ere.results;
            if (results != null) {
                logger.info(
                        "Received {} bytes of {}-compressed {} results from {}.",
                        results.resultBytes.length, results.compression, results.format, event.executionId);
                ArrowStreamReader reader = ArrowUtil.readFrom(results.resultBytes, results.compression);
                query.statement().onExecutionResult(new ExecutionResult(reader, null, null));
            }
            return;
        }

        if (event instanceof Event.ErrorEvent error) {
            query.statement().onExecutionResult(new ExecutionResult(null, new SQLException(error.message), null));
            return;
        }

        logger.warn("Received unknown event kind: {}", event.kind);
    }

    String execute(String sql, WherobotsStatement statement, Store store) {
        String executionId = UUID.randomUUID().toString();
        this.queries.put(executionId, new Query(
                executionId,
                sql,
                statement,
                QueryState.pending));

        String request = JsonUtil.serialize(new ExecuteSqlRequest(
                executionId,
                sql,
                store
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
                (DataFormat) info.get(WherobotsJdbcDriver.FORMAT_PROP),
                (DataCompression) info.getOrDefault(WherobotsJdbcDriver.COMPRESSION_PROP, DataCompression.zstd),
                (GeometryRepresentation) info.get(WherobotsJdbcDriver.GEOMETRY_PROP)
        ));

        logger.info("Retrieving results from {} ...", executionId);
        this.session.send(request);
    }

    void cancel(String executionId) throws SQLException {
        Query query = this.queries.get(executionId);
        if (query == null) {
            return;
        }

        String request = JsonUtil.serialize(new CancelRequest(executionId));
        this.session.send(request);
        logger.info("Cancelled query {}.", executionId);
    }

    @Override
    public Statement createStatement() {
        return new WherobotsStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new WherobotsPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall: stored procedures are not supported");
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // No-op
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        // No-op
    }

    @Override
    public void rollback() throws SQLException {
        // No-op
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
        return new WherobotsDatabaseMetaData(this, this.session);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("setReadOnly: read-only mode is not supported");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // No-op
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTransactionIsolation: transactions are not supported");
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
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("createStatement: unsupported statement parameters");
        }

        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("prepareStatement: unsupported statement parameters");
        }

        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall: stored procedures are not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap: custom type mappings are not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("setHoldability: cursor holdability is not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getHoldability: cursor holdability is not supported");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint: transactions are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint: transactions are not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback: transactions are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint: ransactions are not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("createStatement: unsupported statement parameters");
        }

        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("prepareStatement: unsupported statement parameters");
        }

        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall: stored procedures are not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(sql, columnIndexes)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(sql, columnNames)");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        this.info.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) {
        this.info.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) {
        return (String) this.info.get(name);
    }

    @Override
    public Properties getClientInfo() {
        return this.info;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) {
        // Ignore, users must specify the schema in the SQL query.
    }

    @Override
    public String getSchema() {
        return null;
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
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}

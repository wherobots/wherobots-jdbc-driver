package com.wherobots.db.jdbc;

import com.wherobots.db.jdbc.internal.ExecutionResult;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WherobotsStatement implements Statement {

    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;

    private final BlockingQueue<ExecutionResult> queue;
    private final WherobotsJdbcConnection connection;

    private int timeoutSeconds = DEFAULT_QUERY_TIMEOUT_SECONDS;
    private int maxRows = 0;

    private volatile String executionId;
    private ResultSet results;
    private int updateCount = -1;

    private boolean closeOnCompletion = false;
    private boolean closed = false;

    public WherobotsStatement(WherobotsJdbcConnection connection) {
        this.connection = connection;
        this.queue = new ArrayBlockingQueue<>(1);
    }

    void onExecutionResult(ExecutionResult result) throws InterruptedException {
        this.queue.put(result);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        boolean result = execute(sql);
        if (result) {
            return getResultSet();
        }
        throw new IllegalStateException();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        boolean result = execute(sql);
        if (!result) {
            return getUpdateCount();
        }
        throw new IllegalStateException();
    }

    @Override
    public void close() throws SQLException {
        if (this.results != null) {
            this.results.close();
        }
        this.closed = true;
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // Ignored
    }

    @Override
    public int getMaxRows() {
        return this.maxRows;
    }

    @Override
    public void setMaxRows(int max) {
        if (max < 0) {
            throw new IllegalArgumentException("Invalid maxRows value");
        }

        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (enable) {
            throw new SQLFeatureNotSupportedException("setEscapeProcessing: escape processing is not supported");
        }
    }

    @Override
    public int getQueryTimeout() {
        return this.timeoutSeconds;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        if (seconds < 0) {
            throw new IllegalArgumentException("Invalid queryTimeout value");
        }

        this.timeoutSeconds = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        if (this.executionId != null) {
            this.connection.cancel(this.executionId);
        }
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public void setCursorName(String name) {
        // No-op
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (this.executionId != null) {
            throw new IllegalStateException("This statement has already been executed");
        }

        this.executionId = this.connection.execute(sql, this);

        try {
            ExecutionResult result = this.queue.poll(this.timeoutSeconds, TimeUnit.SECONDS);
            if (result == null) {
                throw new SQLTimeoutException(
                        String.format("No results received after %d second(s)", this.timeoutSeconds));
            }

            if (result.error() != null) {
                throw new SQLException(result.error());
            }

            if (result.result() == null) {
                return false;
            }

            // TODO: differentiate between queries and insert/update/delete results
            this.results = new WherobotsResultSet(this, result.result());
            return true;
        } catch (InterruptedException e) {
            // Pass through
        } catch (IOException e) {
            throw new SQLException(e);
        }

        throw new SQLTimeoutException();
    }

    @Override
    public ResultSet getResultSet() {
        return this.results;
    }

    @Override
    public int getUpdateCount() {
        return this.updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (this.results != null) {
            this.results.close();
            if (this.closeOnCompletion) {
                close();
            }
        }
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // Ignored
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // Ignored
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batches are not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batches are not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batches are not supported");
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor holdability is not supported");
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // Ignored
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() {
        return closeOnCompletion;
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

package com.wherobots.db.jdbc.internal;

import com.wherobots.db.jdbc.WherobotsJdbcConnection;
import com.wherobots.db.jdbc.WherobotsStatement;

public class Query {
    private final String executionId;
    private final String sql;
    private final WherobotsStatement statement;
    private QueryState status;

    public Query(
            String executionId,
            String sql,
            WherobotsStatement statement,
            QueryState status) {
        this.executionId = executionId;
        this.sql = sql;
        this.statement = statement;
        this.status = status;
    }

    public String executionId() {
        return executionId;
    }

    public String sql() {
        return sql;
    }

    public WherobotsStatement statement() {
        return statement;
    }

    public QueryState status() {
        return status;
    }

    public void setStatus(QueryState status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return String.format("Query(%s: %s)", executionId, status);
    }
}

package com.wherobots.db.jdbc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ExecuteSqlRequest {

    public final String kind = "execute_sql";
    public String executionId;
    public String statement;
    public Store store;

    public ExecuteSqlRequest(String executionId, String statement) {
        this(executionId, statement, null);
    }

    public ExecuteSqlRequest(String executionId, String statement, Store store) {
        this.executionId = executionId;
        this.statement = statement;
        this.store = store;
    }
}
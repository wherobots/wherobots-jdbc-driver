package com.wherobots.db.jdbc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CancelRequest {

    public final String kind = "cancel";
    public String executionId;

    public CancelRequest(String executionId) {
        this.executionId = executionId;
    }
}
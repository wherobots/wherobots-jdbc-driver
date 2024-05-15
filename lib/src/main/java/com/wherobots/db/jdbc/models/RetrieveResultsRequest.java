package com.wherobots.db.jdbc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.wherobots.db.DataCompression;
import com.wherobots.db.DataFormat;
import com.wherobots.db.GeometryRepresentation;
import com.wherobots.db.jdbc.WherobotsJdbcConnection;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class RetrieveResultsRequest {

    public final String kind = "retrieve_results";
    public String executionId;
    public DataFormat format;
    public DataCompression compression;
    public GeometryRepresentation geometry;

    public RetrieveResultsRequest(String executionId, DataFormat format, DataCompression compression, GeometryRepresentation geometry) {
        this.executionId = executionId;
        this.format = format;
        this.compression = compression;
        this.geometry = geometry;
    }
}

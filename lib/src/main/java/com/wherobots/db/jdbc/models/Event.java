package com.wherobots.db.jdbc.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.wherobots.db.DataCompression;
import com.wherobots.db.DataFormat;
import com.wherobots.db.GeometryRepresentation;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Event.StateUpdatedEvent.class, name = Event.STATE_UPDATED),
        @JsonSubTypes.Type(value = Event.ExecutionResultEvent.class, name = Event.EXECUTION_RESULT)
})
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Event {

    public static final String STATE_UPDATED = "state_updated";
    public static final String EXECUTION_RESULT = "execution_result";

    public String kind;
    public String executionId;
    public QueryState state;

    public static class StateUpdatedEvent extends Event {}

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class ExecutionResultEvent extends Event {
        public String exceptionOnFailure;
        public Results results;
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Results {
        public byte[] resultBytes;
        public DataCompression compression;
        public DataFormat format;
        public GeometryRepresentation geometry;
        public List<String> geoColumns;
    }
}

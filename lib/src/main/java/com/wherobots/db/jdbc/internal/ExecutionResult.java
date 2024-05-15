package com.wherobots.db.jdbc.internal;

import java.sql.ResultSet;

public record ExecutionResult(ResultSet result, Exception error) {
}

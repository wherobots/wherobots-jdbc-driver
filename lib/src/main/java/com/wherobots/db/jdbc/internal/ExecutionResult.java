package com.wherobots.db.jdbc.internal;

import org.apache.arrow.vector.ipc.ArrowStreamReader;

public record ExecutionResult(ArrowStreamReader result, Exception error) {}

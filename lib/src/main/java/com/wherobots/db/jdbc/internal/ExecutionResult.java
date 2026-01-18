package com.wherobots.db.jdbc.internal;

import com.wherobots.db.jdbc.models.StoreResult;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

public record ExecutionResult(ArrowStreamReader result, Exception error, StoreResult storeResult) {}

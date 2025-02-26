package com.wherobots.db;

public enum SessionType {
    // Allow only a single concurrent connection to the SQL Session
    SINGLE("single"),
    // Allow multiple concurrent connections to the SQL Session
    MULTI("multi");

    public final String name;

    SessionType(String name) {
        this.name = name;
    }
}

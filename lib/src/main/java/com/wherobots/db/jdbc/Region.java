package com.wherobots.db.jdbc;

public enum Region {
    AWS_US_WEST_2("aws-us-west-2");

    public final String name;

    Region(String name) {
        this.name = name;
    }
}

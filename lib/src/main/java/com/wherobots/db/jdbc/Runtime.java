package com.wherobots.db.jdbc;

public enum Runtime {
    SEDONA("TINY"),
    SAN_FRANCISCO("SMALL"),
    NEW_YORK("MEDIUM"),
    CAIRO("LARGE"),
    DELHI("XLARGE"),
    TOKYO("XXLARGE"),

    NEW_YORK_HIMEM("medium-himem"),
    CAIRO_HIMEM("large-himem"),
    DEHLI_HIMEM("xlarge-himem"),
    TOKYO_HIMEM("2x-large-himem"),
    ATLANTIS_HIMEM("4x-large-himem"),

    SEDONA_GPU("tiny-a10-gpu"),
    SAN_FRANCISCO_GPU("small-a10-gpu"),
    NEW_YORK_GPU("medium-a10-gpu");

    public final String name;

    Runtime(String name) {
        this.name = name;
    }
}

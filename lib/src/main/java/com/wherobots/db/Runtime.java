package com.wherobots.db;

public enum Runtime {
    SEDONA("tiny"),
    SAN_FRANCISCO("small"),
    NEW_YORK("medium"),
    CAIRO("large"),
    DELHI("x-large"),
    TOKYO("2x-large"),

    NEW_YORK_HIMEM("medium-himem"),
    CAIRO_HIMEM("large-himem"),
    DEHLI_HIMEM("x-large-himem"),
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

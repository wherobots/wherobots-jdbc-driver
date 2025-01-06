package com.wherobots.db;

public enum Runtime {
    TINY("tiny"),
    SMALL("small"),
    MEDIUM("medium"),
    LARGE("large"),
    X_LARGE("x-large"),
    XX_LARGE("2x-large"),

    MEDIUM_HIMEM("medium-himem"),
    LARGE_HIMEM("large-himem"),
    X_LARGE_HIMEM("x-large-himem"),
    XX_LARGE_HIMEM("2x-large-himem"),
    XXXX_LARGE_HIMEM("4x-large-himem"),

    TINY_A10_GPU("tiny-a10-gpu"),
    SMALL_GPU("small-a10-gpu"),
    MEDIUM_GPU("medium-a10-gpu"),

    @Deprecated SEDONA("tiny"),
    @Deprecated SAN_FRANCISCO("small"),
    @Deprecated NEW_YORK("medium"),
    @Deprecated CAIRO("large"),
    @Deprecated DELHI("x-large"),
    @Deprecated TOKYO("2x-large"),

    @Deprecated NEW_YORK_HIMEM("medium-himem"),
    @Deprecated CAIRO_HIMEM("large-himem"),
    @Deprecated DEHLI_HIMEM("x-large-himem"),
    @Deprecated TOKYO_HIMEM("2x-large-himem"),
    @Deprecated ATLANTIS_HIMEM("4x-large-himem"),

    @Deprecated SEDONA_GPU("tiny-a10-gpu"),
    @Deprecated SAN_FRANCISCO_GPU("small-a10-gpu"),
    @Deprecated NEW_YORK_GPU("medium-a10-gpu");

    public final String name;

    Runtime(String name) {
        this.name = name;
    }
}

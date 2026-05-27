package com.wherobots.db;

public enum Region {
    // Americas
    AWS_US_WEST_2("aws-us-west-2"),
    AWS_US_EAST_1("aws-us-east-1"),
    AWS_US_EAST_2("aws-us-east-2"),

    // EMEA
    AWS_EU_WEST_1("aws-eu-west-1"),

    // APAC
    AWS_AP_SOUTH_1("aws-ap-south-1");

    public final String name;

    Region(String name) {
        this.name = name;
    }

    /**
     * Resolves a user-supplied region property to the value sent to the API.
     *
     * <p>Accepts either an enum constant name (e.g. {@code AWS_US_WEST_2}), for
     * backward compatibility, which is mapped to its API value
     * ({@code aws-us-west-2}); or a raw string — an API value such as
     * {@code aws-us-west-2} or a BYOC region such as {@code byoc-acme-us-east-1}
     * — which is returned unchanged. {@code null} is returned as {@code null} so
     * the caller can omit the region and let the API apply the organization's
     * configured default.
     */
    public static String toApiValue(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Region.valueOf(value).name;
        } catch (IllegalArgumentException e) {
            return value;
        }
    }
}

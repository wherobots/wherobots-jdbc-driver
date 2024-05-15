package com.wherobots.db;

/**
 * Geometry column representation formats.
 * <p>
 * Note that those names are purposefully lowercase to match the SQL Session server's expectations.
 *
 * @author mpetazzoni
 */
public enum GeometryRepresentation {
    wkt,
    wkb,
    ewkt,
    ewkb,
    geojson,
}

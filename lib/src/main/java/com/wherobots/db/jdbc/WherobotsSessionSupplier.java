package com.wherobots.db.jdbc;

import java.util.function.Supplier;

/**
 * Requests and waits for a Wherobots SQL Session.
 *
 * @author mpetazzoni
 */
public class WherobotsSessionSupplier implements Supplier<WherobotsSession> {

    private WherobotsSessionSupplier(String host, Runtime runtime, Region region) {

    }

    @Override
    public WherobotsSession get() {
        return null;
    }

    public static WherobotsSessionSupplier create(String host, Runtime runtime, Region region) {
        return new WherobotsSessionSupplier(host, runtime, region);
    }
}

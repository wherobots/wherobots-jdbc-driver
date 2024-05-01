package com.wherobots.db.jdbc;

import java.io.Closeable;

public class WherobotsSession implements Closeable {

    @Override
    public void close() {

    }

    public boolean isClosed() {
        return false;
    }
}

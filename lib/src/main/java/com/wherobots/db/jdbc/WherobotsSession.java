package com.wherobots.db.jdbc;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.Closeable;
import java.net.URI;
import java.nio.ByteBuffer;

public class WherobotsSession extends WebSocketClient implements Closeable {

    public WherobotsSession(URI uri) {
        super(uri);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {

    }

    @Override
    public void onMessage(String s) {

    }

    @Override
    public void onMessage(ByteBuffer bytes) {

    }

    @Override
    public void onClose(int i, String s, boolean b) {

    }

    @Override
    public void onError(Exception e) {

    }
}

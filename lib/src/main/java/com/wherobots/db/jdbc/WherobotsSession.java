package com.wherobots.db.jdbc;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ServerHandshake;

import java.io.Closeable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class WherobotsSession extends WebSocketClient implements Iterable<Frame>, Closeable {

    public static final Logger logger = Logger.getLogger(WherobotsSession.class.getName());

    private final BlockingQueue<Frame> queue;

    public WherobotsSession(URI uri) {
        super(uri);
        this.queue = new ArrayBlockingQueue<>(1);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        logger.info("WebSocket connection opened.");
    }

    private void forward(Frame message) {
        try {
            this.queue.put(message);
        } catch (InterruptedException e) {
            logger.warning("Interrupted while handling inbound message");
            this.close(CloseFrame.UNEXPECTED_CONDITION);
        }
    }

    @Override
    public void onMessage(String s) {
        this.forward(new Frame(s, null, null));
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        this.forward(new Frame(null, bytes, null));
    }

    @Override
    public void onClose(int code, String reason, boolean byHost) {
        logger.info(String.format("WebSocket connection closed (%d: %s).", code, reason));
    }

    @Override
    public void onError(Exception e) {
        this.forward(new Frame(null, null, e));
    }

    @Override
    public Iterator<Frame> iterator() {
        return this.queue.iterator();
    }
}

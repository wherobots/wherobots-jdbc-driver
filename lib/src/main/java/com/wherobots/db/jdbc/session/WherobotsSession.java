package com.wherobots.db.jdbc.session;

import com.wherobots.db.jdbc.internal.Frame;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A WebSocket connection to a running Wherobots SQL Session instance.
 * <p>
 * This class wraps a WebSocket connection to a Wherobots SQL Session server and presents itself as an iterable of
 * {@link Frame} messages.
 * </p>
 *
 * @author mpetazzoni
 */
public class WherobotsSession extends WebSocketClient implements Iterable<Frame>, Closeable {

    public static final Logger logger = LoggerFactory.getLogger(WherobotsSession.class);

    private final BlockingQueue<Frame> queue;

    public WherobotsSession(URI uri, Map<String, String> headers) throws IOException, InterruptedException {
        super(uri, headers);
        this.queue = new ArrayBlockingQueue<>(1);
        if (!this.connectBlocking()) {
            throw new IOException("Failed to connect to SQL Session!");
        }
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        logger.info("WebSocket connection opened.");
    }

    private void forward(Frame message) {
        try {
            this.queue.put(message);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while handling inbound message");
            this.close(CloseFrame.UNEXPECTED_CONDITION);
        }
    }

    @Override
    public void onMessage(String s) {
        logger.info("< {}", s);
        this.forward(new Frame(s, null, null));
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        logger.info("< {}", bytes);
        this.forward(new Frame(null, bytes, null));
    }

    @Override
    public void onClose(int code, String reason, boolean byHost) {
        logger.info("WebSocket connection closed ({}: {}).", code, reason);
        this.forward(new Frame(null, null, new IOException("WebSocket connection closed")));
    }

    @Override
    public void onError(Exception e) {
        this.forward(new Frame(null, null, e));
    }

    @Override
    public void send(String text) {
        logger.info("> {}", text);
        super.send(text);
    }

    @Override
    public Iterator<Frame> iterator() {
        return this.queue.iterator();
    }
}

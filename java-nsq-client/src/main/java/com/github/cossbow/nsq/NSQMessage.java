package com.github.cossbow.nsq;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.ReferenceCountUtil;


import java.io.IOException;
import java.io.InputStream;

public class NSQMessage<T> {

    private CharSequence id;
    private int attempts;
    private long timestamp;
    private CompressType compress;

    private ByteBuf buf;
    private final Connection connection;

    private volatile InputStream body;
    private volatile boolean hasRead;
    private volatile T obj;

    public NSQMessage(Connection connection) {
        this.connection = connection;
    }

    /**
     * Finished processing this message, let com.github.cossbow.nsq know so it doesnt get reprocessed.
     */
    public void finished() {
        connection.command(NSQCommand.finish(this.id));
    }

    public void touch() {
        connection.command(NSQCommand.touch(this.id));
    }

    /**
     * indicates a problem with processing, puts it back on the queue.
     */
    public void requeue(int timeoutMillis) {
        connection.command(NSQCommand.requeue(this.id, timeoutMillis));
    }

    public void requeue() {
        requeue(0);
    }


    //

    public Connection getConnection() {
        return connection;
    }

    public CharSequence getId() {
        return id;
    }

    void setId(CharSequence id) {
        this.id = id;
    }

    public int getAttempts() {
        return attempts;
    }

    void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getTimestamp() {
        return timestamp;
    }

    void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public CompressType getCompress() {
        return compress;
    }

    public void setCompress(CompressType compress) {
        this.compress = compress;
    }

    void setNanoseconds(long nanoseconds) {
        this.timestamp = Math.floorDiv(nanoseconds, 1000_000L);
    }

    void setBuf(ByteBuf buf) {
        this.buf = buf;
    }

    public byte[] getMessage() {
        try (var in = newMessageStream()) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException("message buf cannot read");
        }
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

    public T getObj() {
        return obj;
    }

    //

    public synchronized InputStream newMessageStream() throws IOException {
        if (!hasRead) {
            try {
                var in = new ByteBufInputStream(buf, true) {
                    @Override
                    public void close() throws IOException {
                        try {
                            super.close();
                        } catch (Throwable ignore) {
                        }
                    }
                };
                body = compress.input.apply(in);
            } finally {
                hasRead = true;
            }
        }
        return body;
    }

    synchronized void release() {
        if (!hasRead) {
            try {
                ReferenceCountUtil.safeRelease(buf);
            } finally {
                hasRead = true;
            }
        }
    }

}

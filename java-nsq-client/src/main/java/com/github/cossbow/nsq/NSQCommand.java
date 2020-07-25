package com.github.cossbow.nsq;


import com.github.cossbow.nsq.exceptions.NSQDException;
import com.github.cossbow.nsq.util.ThrowoutConsumer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Stream;


public class NSQCommand {

    private static final String LINE_SUFFIX = "\n";

    private final String line;

    private final CompressType compress;

    private final Runnable bufCreator;

    private volatile ByteBuf buf;

    private volatile int startIndex;

    //

    private NSQCommand(String line, CompressType compress) {
        this.line = line;
        this.compress = CompressType.null2Non(compress);
        this.bufCreator = this::init;
    }

    private NSQCommand(String line, CompressType compress, byte[] data) {
        this.line = line;
        this.compress = CompressType.null2Non(compress);
        this.bufCreator = () -> writeData(data);
    }

    private NSQCommand(String line, CompressType compress, Collection<byte[]> data) {
        this.line = line;
        this.compress = CompressType.null2Non(compress);
        this.bufCreator = () -> writeData(data);
    }

    private NSQCommand(String line, CompressType compress, ThrowoutConsumer<OutputStream, IOException> callback) {
        this.line = line;
        this.compress = CompressType.null2Non(compress);
        this.bufCreator = () -> writeCallback(callback);
    }

    private NSQCommand(String line, CompressType compress, Stream<ThrowoutConsumer<OutputStream, IOException>> callbackStream) {
        this.line = line;
        this.compress = compress;
        this.bufCreator = () -> writeCallbackStream(callbackStream);
    }

    //

    private void init() {
        buf = ByteBufAllocator.DEFAULT.buffer();
        try {

            if (line.endsWith(LINE_SUFFIX)) {
                buf.writeBytes(line.getBytes(StandardCharsets.UTF_8));
            } else {
                buf.writeBytes(line.getBytes(StandardCharsets.UTF_8))
                        .writeBytes(LINE_SUFFIX.getBytes());
            }
            startIndex = buf.writerIndex();

        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(buf);
            throw e;
        }
    }

    private void writeData(byte[] data) {
        init();
        try {
            var ed = compress.decode(data);
            buf.writeInt(ed.length);
            buf.writeBytes(ed);
        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(buf);
            throw new NSQDException(e);
        }
    }

    private void writeData(Collection<byte[]> data) {
        init();
        // compress
        var da = new byte[data.size()][];
        var i = 0;
        try {
            for (var d : data) {
                da[i++] = compress.encode(d);
            }
        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(buf);
            throw new NSQDException(e);
        }
        //for MPUB messages.
        if (da.length > 1) {
            //write total bodysize and message size
            int bodySize = 4; //4 for total messages int.
            for (var d : da) {
                bodySize += 4; //message size
                bodySize += d.length;
            }
            buf.writeInt(bodySize);
            buf.writeInt(da.length);
        }

        for (var d : da) {
            buf.writeInt(d.length);
            buf.writeBytes(d);
        }
    }

    private void writeCallback(ThrowoutConsumer<OutputStream, IOException> callback) {
        init();
        buf.writeInt(0);

        var os = new ByteBufOutputStream(buf);
        try (var out = compress.output.apply(os)) {
            callback.accept(out);
        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(buf);
            throw new NSQDException(e);
        }
        var end = buf.writerIndex(); // 结束位置
        var len = os.writtenBytes();    // 数据长度

        buf.writerIndex(startIndex);        // 写入长度
        buf.writeInt(len);
        buf.writerIndex(end);

    }

    private void writeCallbackStream(Stream<ThrowoutConsumer<OutputStream, IOException>> callbackStream) {
        init();
        buf.writeInt(0);            // 总大小
        buf.writeInt(0);            // 总数量

        var lenArr = callbackStream.mapToInt(callback -> {
            var is = buf.writerIndex(); // size位置
            buf.writeInt(0);

            var os = new ByteBufOutputStream(buf);
            try (var out = compress.output.apply(os)) {
                callback.accept(out);
            } catch (IOException e) {
                ReferenceCountUtil.safeRelease(buf);
                throw new NSQDException(e);
            }
            var ie = buf.writerIndex(); // 结束位置
            var len = os.writtenBytes();

            buf.writerIndex(is);
            buf.writeInt(len);
            buf.writerIndex(ie);

            return len + 4;
        }).toArray();

        var end = buf.writerIndex();
        buf.writerIndex(startIndex);            // 写入长度
        int len = 4;
        for (var it : lenArr) {
            len += it;
        }
        buf.writeInt(len);              // 总大小
        buf.writeInt(lenArr.length);    // 总数量
        buf.writerIndex(end);
    }

    //

    public ByteBuf getBuf() {
        if (null == buf) {
            bufCreator.run();
        }
        return buf;
    }

    public void release() {
        ReferenceCountUtil.safeRelease(buf);
    }

    @Override
    public String toString() {
        return line;
    }


    // Identify creates a new Command to provide information about the client.  After connecting,
    // it is generally the first message sent.
    //
    // The supplied body should be a map marshaled into JSON to provide some flexibility
    // for this command to evolve over time.
    //
    // See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
    // on the supported options
    public static NSQCommand identify(byte[] body) {
        return new NSQCommand("IDENTIFY", null, body);
    }

    public static NSQCommand identify(ThrowoutConsumer<OutputStream, IOException> callback) {
        return new NSQCommand("IDENTIFY", null, callback);
    }

    // Touch creates a new Command to reset the timeout for
    // a given message (by id)
    public static NSQCommand touch(CharSequence messageID) {
        return new NSQCommand("TOUCH " + messageID, null);
    }

    // Finish creates a new Command to indiciate that
    // a given message (by id) has been processed successfully
    public static NSQCommand finish(CharSequence messageID) {
        return new NSQCommand("FIN " + messageID, null);
    }

    // Subscribe creates a new Command to subscribe to the given topic/channel
    public static NSQCommand subscribe(String topic, String channel) {
        return new NSQCommand("SUB " + topic + " " + channel, null);
    }

    // StartClose creates a new Command to indicate that the
    // client would like to start a close cycle.  nsqd will no longer
    // send messages to a client in this state and the client is expected
    // finish pending messages and close the connection
    public static NSQCommand startClose() {
        return new NSQCommand("CLS", null);
    }

    public static NSQCommand requeue(CharSequence messageID, int timeoutMillis) {
        return new NSQCommand("REQ " + messageID + " " + timeoutMillis, null);
    }

    // Nop creates a new Command that has no effect server side.
    // Commonly used to respond to heartbeats
    public static NSQCommand nop() {
        return new NSQCommand("NOP", null);
    }

    // Ready creates a new Command to specify
    // the number of messages a client is willing to receive
    public static NSQCommand ready(int rdy) {
        return new NSQCommand("RDY " + rdy, null);
    }

    // Publish creates a new Command to write a message to a given topic
    public static NSQCommand publish(String topic, CompressType compress, byte[] message) {
        return new NSQCommand("PUB " + topic, compress, message);
    }

    /**
     * @throws NSQDException write buffer error
     */
    public static NSQCommand publish(String topic, CompressType compress, ThrowoutConsumer<OutputStream, IOException> callback) {
        return new NSQCommand("PUB " + topic, compress, callback);
    }

    // Publish creates a new Command to write a deferred message to a given topic
    public static NSQCommand publish(String topic, CompressType compress, int deferTime, byte[] message) {
        return new NSQCommand("DPUB " + topic + " " + deferTime, compress, message);
    }

    /**
     * @throws NSQDException write buffer error
     */
    public static NSQCommand publish(String topic, CompressType compress, int deferTime, ThrowoutConsumer<OutputStream, IOException> callback) {
        return new NSQCommand("DPUB " + topic + " " + deferTime, compress, callback);
    }

    // MultiPublish creates a new Command to write more than one message to a given topic
    // (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
    // Note: can only be used with more than 1 bodies!
    public static NSQCommand multiPublish(String topic, CompressType compress, Collection<byte[]> bodies) {
        return new NSQCommand("MPUB " + topic, compress, bodies);
    }

    /**
     * @throws NSQDException write buffer error
     */
    public static NSQCommand multiPublish(String topic, CompressType compress, Stream<ThrowoutConsumer<OutputStream, IOException>> callbackStream) {
        return new NSQCommand("MPUB " + topic, compress, callbackStream);
    }

}

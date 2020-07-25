package com.github.cossbow.nsq;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CompressTest {
    static {
        System.out.println("===============");
    }

    private void log(String fmt, Object... args) {
        var s = String.format(fmt, args);
        System.out.println(s);
    }

    private void log(String s) {
        System.out.println(s);
    }

    String getTestData() {
        try {
            var is = CompressTest.class.getResourceAsStream("/test-data.json");
            return new String(is.readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    void testCompress(CompressType compress, byte[] bs) throws IOException {
        log("test with byte[]");
        var cd = compress.encode(bs);
        log("%s: %d compress to %d", compress, bs.length, cd.length);
        var ed = compress.decode(cd);
        log("%s: %d decompress to %d", compress, cd.length, ed.length);
    }

    void testCompress(CompressType compress, ByteBuf bs) throws IOException {
        log("test with ByteBuf");
        var cd = compress.encode(bs);
        bs.resetReaderIndex();
        cd.resetReaderIndex();
        log("%s: %d compress to %d", compress, bs.readableBytes(), cd.readableBytes());
        var ed = compress.decode(cd);
        cd.resetReaderIndex();
        ed.resetReaderIndex();
        log("%s: %d decompress to %d", compress, cd.readableBytes(), ed.readableBytes());
    }

    void testCompress(CompressType compress, ByteBuffer bs) throws IOException {
        log("test with ByteBuffer");
        var cd = compress.encode(bs);
        log("%s: %d compress to %d", compress, bs.remaining(), cd.remaining());
        var ed = compress.decode(cd);
        log("%s: %d decompress to %d", compress, cd.remaining(), ed.remaining());
    }


    @Test
    public void bytes() throws IOException {
        var bs = getTestData().getBytes();

        for (var compress : CompressType.values()) {
            testCompress(compress, bs);
        }

    }

    @Test
    public void buf() throws IOException {
        var s = getTestData();
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeCharSequence(s, StandardCharsets.UTF_8);

        for (var compress : CompressType.values()) {
            testCompress(compress, buf);
        }

    }

    @Test
    public void buffer() throws IOException {
        var s = getTestData();
        var buffer = ByteBuffer.wrap(s.getBytes());

        for (var compress : CompressType.values()) {
            testCompress(compress, buffer);
        }

    }


}

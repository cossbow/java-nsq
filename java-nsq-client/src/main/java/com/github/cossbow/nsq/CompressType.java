package com.github.cossbow.nsq;

import com.github.cossbow.nsq.util.ThrowoutFunction;
import io.netty.buffer.*;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;


public enum CompressType {

    Non(ThrowoutFunction.identity(), ThrowoutFunction.identity()),

    Snappy(in -> new SnappyFramedInputStream(in), out -> new SnappyFramedOutputStream(out)),

    Deflate(in -> new InflaterInputStream(in), out -> new DeflaterOutputStream(out)),

    ;

    public final ThrowoutFunction<InputStream, InputStream, IOException> input;
    public final ThrowoutFunction<OutputStream, OutputStream, IOException> output;

    CompressType(
            ThrowoutFunction<InputStream, InputStream, IOException> input,
            ThrowoutFunction<OutputStream, OutputStream, IOException> output) {
        this.input = input;
        this.output = output;
    }


    public byte[] decode(byte[] s) throws IOException {
        if (Non == this) return s;
        var is = new ByteArrayInputStream(s);
        try (var in = input.apply(is)) {
            return in.readAllBytes();
        }
    }

    public ByteBuf decode(ByteBuf s) throws IOException {
        var is = new ByteBufInputStream(s);
        var b = ByteBufAllocator.DEFAULT.buffer(1024);
        var out = new ByteBufOutputStream(b);
        try (var in = input.apply(is)) {
            in.transferTo(out);
        }
        return b;
    }

    public ByteBuffer decode(ByteBuffer s) throws IOException {
        return decode(Unpooled.wrappedBuffer(s)).nioBuffer();
    }


    public byte[] encode(byte[] s) throws IOException {
        var os = new ByteArrayOutputStream();
        try (var out = output.apply(os)) {
            out.write(s);
        }
        return os.toByteArray();
    }

    public ByteBuf encode(ByteBuf s) throws IOException {
        var is = new ByteBufInputStream(s);
        var b = ByteBufAllocator.DEFAULT.buffer(1024);
        var os = new ByteBufOutputStream(b);
        try (var out = output.apply(os)) {
            is.transferTo(out);
        }
        return b;
    }

    public ByteBuffer encode(ByteBuffer s) throws IOException {
        return encode(Unpooled.wrappedBuffer(s)).nioBuffer();
    }

    //

    public static CompressType valueOf(int i) {
        var values = values();
        if (0 <= i && i < values.length) {
            return values[i];
        }
        throw new IllegalArgumentException("illegal ordinal " + i);
    }

    public static CompressType valueOf(int i, CompressType defVal) {
        var values = values();
        if (0 <= i && i < values.length) {
            return values[i];
        }
        return defVal;
    }

    public static CompressType null2Non(CompressType compress) {
        return null == compress ? Non : compress;
    }


}

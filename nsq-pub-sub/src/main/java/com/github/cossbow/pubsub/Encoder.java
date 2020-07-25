package com.github.cossbow.pubsub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public
interface Encoder {

    <T> void encode(OutputStream os, T v) throws IOException;

    default <T> byte[] encode(T v) throws IOException {
        var os = new ByteArrayOutputStream();
        encode(os, v);
        return os.toByteArray();
    }

}

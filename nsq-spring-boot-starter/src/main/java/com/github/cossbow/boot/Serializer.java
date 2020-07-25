package com.github.cossbow.boot;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer extends Encoder, Decoder {

    /**
     * @param os  {@link OutputStream}
     * @param v
     * @param <T>
     * @throws IOException
     */
    @Override
    <T> void encode(OutputStream os, T v) throws IOException;

    /**
     * @param is   {@link InputStream}
     * @param type
     * @param <T>
     * @return
     * @throws IOException
     */
    @Override
    <T> T decode(InputStream is, Class<T> type) throws IOException;


    //
    //
    //


    static Serializer jdk() {
        return new JdkSerializer();
    }


}

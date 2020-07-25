package com.github.cossbow.pubsub;

import java.io.*;

public class JdkSerializer implements Serializer {

    @Override
    public <T> void encode(OutputStream os, T v) throws IOException {
        try (var oo = new ObjectOutputStream(os)) {
            oo.writeObject(v);
        }
    }

    @Override
    public <T> T decode(InputStream is, Class<T> type) throws IOException {
        try (var in = new ObjectInputStream(is)) {
            @SuppressWarnings("unchecked")
            var t = (T) in.readObject();
            return t;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

}

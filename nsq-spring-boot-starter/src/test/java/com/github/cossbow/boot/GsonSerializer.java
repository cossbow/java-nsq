package com.github.cossbow.boot;

import com.google.gson.Gson;

import java.io.*;

public class GsonSerializer implements Serializer {
    private final Gson gson;

    public GsonSerializer(Gson gson) {
        this.gson = gson;
    }

    @Override
    public <T> void encode(OutputStream os, T v) throws IOException {
        gson.toJson(v, new OutputStreamWriter(os));
    }

    @Override
    public <T> T decode(InputStream is, Class<T> type) throws IOException {
        return gson.fromJson(new InputStreamReader(is), type);
    }

}

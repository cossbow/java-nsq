package com.github.cossbow.sample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cossbow.pubsub.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JacksonSerializer implements Serializer {

    private final ObjectMapper objectMapper;

    public JacksonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public <T> void encode(OutputStream os, T v) throws IOException {
        objectMapper.writeValue(os, v);
    }

    @Override
    public <T> T decode(InputStream is, Class<T> type) throws IOException {
        return objectMapper.readValue(is, type);
    }

}

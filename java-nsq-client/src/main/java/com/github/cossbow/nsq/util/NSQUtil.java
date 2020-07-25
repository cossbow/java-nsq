package com.github.cossbow.nsq.util;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

final
public class NSQUtil {
    private NSQUtil() {
    }


    public static final ScheduledExecutorService SCHEDULER
            = Executors.newSingleThreadScheduledExecutor();

    public final static ExecutorService DEFAULT_EXECUTOR;

    public static final HttpClient CLIENT;

    private static final Gson gson;

    static {
        var s = System.getSecurityManager();

        var group = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        var threadNumber = new AtomicInteger(1);
        DEFAULT_EXECUTOR = Executors.newCachedThreadPool(r -> {
            var t = new Thread(group, r, "com.github.cossbow.nsq-c-" + threadNumber.getAndIncrement());
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        });

        CLIENT = HttpClient.newBuilder().executor(DEFAULT_EXECUTOR).build();

        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }


    public static <T> String toJson(T v) {
        return gson.toJson(v);
    }

    public static <T> T fromJson(String src, Class<T> type) {
        return gson.fromJson(src, type);
    }

    public static <T> T fromJson(URL src, Class<T> type) {
        try {
            return gson.fromJson(new InputStreamReader(src.openStream()), type);
        } catch (IOException e) {
            throw new IllegalArgumentException("read fail", e);
        }
    }

    public static <T> T fromJson(InputStream src, Class<T> type) {
        try (src) {
            return gson.fromJson(new InputStreamReader(src), type);
        } catch (IOException e) {
            throw new IllegalArgumentException("read fail", e);
        }
    }


    public static <T> void writeJson(OutputStream out, T v) {
        gson.toJson(v, new OutputStreamWriter(out));
    }


    public static <T> ThrowoutFunction<InputStream, T, IOException> decode(Class<T> type) {
        return in -> fromJson(in, type);
    }


    //
    //
    //

    public static <T> CompletableFuture<T> get(String uri, Class<T> type) {
        var req = HttpRequest.newBuilder().uri(URI.create(uri)).GET().build();
        return CLIENT.sendAsync(req, HttpResponse.BodyHandlers.ofInputStream())
                .thenApply(resp -> {
                    if (200 != resp.statusCode()) {
                        throw new IllegalStateException("HTTP " + resp.statusCode() + ": " + uri);
                    }
                    return fromJson(resp.body(), type);
                });
    }

    public static CompletableFuture<String> post(String uri) {
        var req = HttpRequest.newBuilder().uri(URI.create(uri))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        return CLIENT.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body);
    }


    public static <T> CompletableFuture<List<T>> collectList(Iterator<CompletableFuture<T>> s) {
        return collectList(s, Function.identity());
    }

    public static <T, R> CompletableFuture<List<R>> collectList(Iterator<CompletableFuture<T>> s, Function<T, R> mapper) {
        var result = new LinkedList<R>();
        var futures = new ArrayList<CompletableFuture<Void>>();
        while (s.hasNext()) {
            var tf = s.next();
            futures.add(tf.thenAccept(t -> {
                synchronized (result) {
                    result.add(mapper.apply(t));
                }
            }));
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> result);
    }


    //


}

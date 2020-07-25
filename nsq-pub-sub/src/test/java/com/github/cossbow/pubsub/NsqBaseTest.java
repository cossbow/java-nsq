package com.github.cossbow.pubsub;

import com.github.cossbow.nsq.CompressType;
import com.github.cossbow.nsq.NSQConfig;
import com.github.cossbow.nsq.lookup.DefaultNSQLookup;
import com.github.cossbow.nsq.lookup.NSQLookup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class NsqBaseTest {
    static {
        System.setProperty("logging.config", "classpath:log4j.xml");
    }

    final HttpClient client = HttpClient.newBuilder().build();

    NSQLookup nsqLookup = nsqLookup();


    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(16);

    protected News create(int id) {
        News news = new News();
        news.setId(id);
        news.setTitle("测试新闻标题" + id);
        news.setContent("就这么点内容！你看着办！" + id);
        news.setCreatedTime(Instant.now());
        news.setPublishedTime(OffsetDateTime.now());
        return news;
    }


    protected CompletableFuture<String> get(String url) {
        var req = HttpRequest.newBuilder(URI.create(url)).build();
        var f = client.sendAsync(req, HttpResponse.BodyHandlers.ofString());
        return f.thenApply(HttpResponse::body);
    }


    NSQLookup nsqLookup() {
        String lookupAddress = "127.0.0.1:4161";
        var nsqLookup = new DefaultNSQLookup();
        var uri = URI.create("http://" + lookupAddress);
        nsqLookup.addLookupAddress(uri.getHost(), uri.getPort());
        return nsqLookup;
    }

    NsqPublisher nsqPublisher() {
        return nsqPublisher(CompressType.Non);
    }

    NsqPublisher nsqPublisher(CompressType compress) {
        var config = new NSQConfig();
        config.setCompress(compress);
        return new NsqPublisherImpl(nsqLookup, config);
    }


    NsqSubscriber nsqSubscriber() {
        return new NsqSubscriberImpl(nsqLookup,
                6000, 3, 1000,
                2, "java");
    }

}

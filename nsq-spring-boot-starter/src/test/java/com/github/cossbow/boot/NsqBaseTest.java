package com.github.cossbow.boot;

import org.junit.Test;

import java.io.IOException;
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

    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10);

    protected News create(int id) {
        News news = new News();
        news.setId(id);
        news.setTitle("测试新闻标题" + id);
        news.setContent("就这么点内容！你看着办！" + id);
        news.setCreatedTime(Instant.now());
        news.setPublishedTime(OffsetDateTime.now());
        return news;
    }

    @Test
    public void serial() throws IOException {
        var news = create(1001);
        var data = NsqUtil.getDefaultEncoder().encode(news);
        System.out.println(new String(data));
        var n2 = NsqUtil.getDefaultDecoder().decode(data, News.class);
        System.out.println(n2);
    }


    protected CompletableFuture<String> get(String url) {
        var req = HttpRequest.newBuilder(URI.create(url)).build();
        var f = client.sendAsync(req, HttpResponse.BodyHandlers.ofString());
        return f.thenApply(HttpResponse::body);
    }

    protected long limitAttemps(int attempts, int attemptsLimit) {
        if (attempts < attemptsLimit) {
            return 200;
        } else {
            return 0;
        }
    }
}

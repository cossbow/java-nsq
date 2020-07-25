package com.github.cossbow.nsq.lookup;

import com.github.cossbow.nsq.ServerAddress;
import com.github.cossbow.nsq.util.NSQUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultNSQLookup implements NSQLookup {
    private static final Logger log = LoggerFactory.getLogger(DefaultNSQLookup.class);


    private Set<String> addresses = ConcurrentHashMap.newKeySet();


    @Override
    public void addLookupAddress(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }


    @Override
    public Set<ServerAddress> lookup(String topic) {
        try {
            return lookupAsync(topic).join();
        } catch (CompletionException e) {
            log.error("lookup exception", e.getCause());
            return ConcurrentHashMap.newKeySet();
        }
    }

    private CompletableFuture<List<ServerAddress>> lookupProducers(Function<String, String> addressMapper) {
        var lookupAddresses = getLookupAddresses();
        var it = lookupAddresses.stream().map(addressMapper)
                .map(url -> NSQUtil.get(url, LookupResponse.class))
                .iterator();
        return NSQUtil.collectList(it, LookupResponse::getProducers)
                .thenApply(ll -> ll.stream().flatMap(Collection::stream)
                        .map(LookupResponse.Producer::toServerAddress)
                        .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Set<ServerAddress>> lookupAsync(String topic) {
        String topicEncoded = URLEncoder.encode(topic, StandardCharsets.UTF_8);
        var future = lookupProducers(addr -> addr + "/lookup?topic=" + topicEncoded);

        var completeFuture = new CompletableFuture<Set<ServerAddress>>();
        future.thenAccept(addresses -> {
            if (addresses.isEmpty()) {
                log.debug("Unable to find any NSQ servers in lookup server: {} on topic: {}, will create and try again...",
                        this.addresses, topic);
                createTopic(topic).whenComplete((set, e) -> {
                    if (null == e) {
                        completeFuture.complete(set);
                    } else {
                        if (e instanceof CompletionException) e = e.getCause();
                        completeFuture.completeExceptionally(e);
                    }
                });
            } else {
                completeFuture.complete(new HashSet<>(addresses));
            }
        }).exceptionally(e -> {
            if (e instanceof CompletionException) e = e.getCause();
            completeFuture.completeExceptionally(e);
            return null;
        });

        return completeFuture;
    }


    public CompletableFuture<List<ServerAddress>> lookupNodes() {
        return lookupProducers(addr -> addr + "/nodes");
    }

    public CompletableFuture<Set<ServerAddress>> lookupNodeAsync() {
        return lookupNodes().thenApply(HashSet::new);
    }

    private CompletableFuture<Set<ServerAddress>> createTopic(String topic) {
        log.debug("lookup create topic[{}]", topic);
        String topicEncoded = URLEncoder.encode(topic, StandardCharsets.UTF_8);
        var future = new CompletableFuture<Set<ServerAddress>>();
        try {
            lookupNodes().thenAccept(serverAddresses -> {
                NSQUtil.collectList(serverAddresses.stream().map(serverAddress -> {
                    var url = serverAddress.httpAddress() + "/topic/create?topic=" + topicEncoded;
                    return NSQUtil.post(url).thenApply(s -> serverAddress);
                }).iterator()).thenAccept(li -> {
                    future.complete(new HashSet<>(li));
                }).exceptionally(e -> {
                    future.completeExceptionally(e);
                    return null;
                });
            }).exceptionally(e -> {
                future.completeExceptionally(e);
                return null;
            });
        } catch (Throwable e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public Set<String> getLookupAddresses() {
        return addresses;
    }


    static class LookupResponse {

        private List<Producer> producers;

        public List<Producer> getProducers() {
            return producers;
        }

        public void setProducers(List<Producer> producers) {
            this.producers = producers;
        }

        static class Producer {
            private String broadcastAddress;
            private int tcpPort;
            private int httpPort;

            public String getBroadcastAddress() {
                return broadcastAddress;
            }

            public void setBroadcastAddress(String broadcastAddress) {
                this.broadcastAddress = broadcastAddress;
            }

            public int getTcpPort() {
                return tcpPort;
            }

            public void setTcpPort(int tcpPort) {
                this.tcpPort = tcpPort;
            }

            public int getHttpPort() {
                return httpPort;
            }

            public void setHttpPort(int httpPort) {
                this.httpPort = httpPort;
            }

            //
            public ServerAddress toServerAddress() {
                return new ServerAddress(broadcastAddress, tcpPort, httpPort);
            }
        }

    }

}

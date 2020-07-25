package com.github.cossbow.nsq;

import com.github.cossbow.nsq.exceptions.BadMessageException;
import com.github.cossbow.nsq.exceptions.BadTopicException;
import com.github.cossbow.nsq.exceptions.NSQException;
import com.github.cossbow.nsq.exceptions.NoConnectionsException;
import com.github.cossbow.nsq.frames.ErrorFrame;
import com.github.cossbow.nsq.frames.NSQFrame;
import com.github.cossbow.nsq.pool.ConnectionPoolFactory;
import com.github.cossbow.nsq.util.ThrowoutConsumer;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.github.cossbow.nsq.NSQCommand.multiPublish;
import static com.github.cossbow.nsq.NSQCommand.publish;

public class NSQProducer {
    private Set<ServerAddress> addresses = ConcurrentHashMap.newKeySet();
    private int roundRobinCount = 0;
    private volatile boolean started = false;
    private GenericKeyedObjectPoolConfig<Connection> poolConfig = null;
    private GenericKeyedObjectPool<ServerAddress, Connection> pool;
    private NSQConfig config = new NSQConfig();
    private int connectionRetries = 5;

    public NSQProducer start() {
        if (!started) {
            started = true;
            createPool();
        }
        return this;
    }

    private void createPool() {
        if (poolConfig == null) {
            poolConfig = new GenericKeyedObjectPoolConfig<>();
            poolConfig.setTestOnBorrow(true);
            poolConfig.setJmxEnabled(false);
        }
        pool = new GenericKeyedObjectPool<>(new ConnectionPoolFactory(config), poolConfig);
    }

    protected Connection getConnection() throws NoConnectionsException {
        int c = 0;
        while (c < connectionRetries) {
            ServerAddress[] serverAddresses = addresses.toArray(new ServerAddress[0]);
            if (serverAddresses.length != 0) {
                try {
                    return pool.borrowObject(serverAddresses[roundRobinCount++ % serverAddresses.length]);
                } catch (NoSuchElementException e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ix) {
                        throw new NoConnectionsException("Could not acquire a connection to a server", ix);
                    }
                } catch (Exception ex) {
                    throw new NoConnectionsException("Could not acquire a connection to a server", ex);
                }
            }
        }
        throw new IllegalStateException("No server configured for producer");
    }

    /**
     * produce multiple messages.
     */
    public void produceMulti(String topic, List<byte[]> messages) throws TimeoutException, NSQException, InterruptedException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        if (messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, messages.get(0));
            return;
        }

        Connection c = this.getConnection();
        try {
            NSQCommand command = multiPublish(topic, config.getCompress(), messages);


            NSQFrame frame = c.commandAndWait(command);
            checkErrorFrame(frame);
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public void produceMulti(String topic, Stream<ThrowoutConsumer<OutputStream, IOException>> callbackStream)
            throws TimeoutException, NSQException, InterruptedException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        if (callbackStream == null) {
            return;
        }

        Connection c = this.getConnection();
        try {
            NSQCommand command = multiPublish(topic, config.getCompress(), callbackStream);


            NSQFrame frame = c.commandAndWait(command);
            checkErrorFrame(frame);
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public void produce(String topic, byte[] message) throws NSQException, TimeoutException, InterruptedException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        Connection c = getConnection();
        try {
            NSQCommand command = publish(topic, config.getCompress(), message);
            NSQFrame frame = c.commandAndWait(command);
            checkErrorFrame(frame);
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public CompletableFuture<Void> produceAsync(String topic, int defer, byte[] message) throws NoConnectionsException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        Connection c = getConnection();
        try {
            var command = defer > 0 ?
                    publish(topic, config.getCompress(), defer, message) :
                    publish(topic, config.getCompress(), message);
            try {
                var channelFuture = c.command(command);
                var future = new CompletableFuture<Void>();
                channelFuture.addListener(_f -> {
                    if (_f.isSuccess()) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(_f.cause());
                    }
                });
                return future;
            } catch (Throwable e) {
                command.release();
                return CompletableFuture.failedFuture(e);
            }
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public CompletableFuture<Void> produceAsync(String topic, byte[] message) throws NoConnectionsException {
        return produceAsync(topic, 0, message);
    }

    public CompletableFuture<Void> produceAsync(String topic, int defer, ThrowoutConsumer<OutputStream, IOException> callback) throws NoConnectionsException {
        if (!started) {
            throw new IllegalStateException("Producer must be started before producing messages!");
        }
        Connection c = getConnection();
        try {
            var command = defer > 0 ?
                    publish(topic, config.getCompress(), defer, callback) :
                    publish(topic, config.getCompress(), callback);
            try {
                var channelFuture = c.command(command);
                var future = new CompletableFuture<Void>();
                channelFuture.addListener(_f -> {
                    if (_f.isSuccess()) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(_f.cause());
                    }
                });
                return future;
            } catch (Throwable e) {
                command.release();
                return CompletableFuture.failedFuture(e);
            }
        } finally {
            pool.returnObject(c.getServerAddress(), c);
        }
    }

    public CompletableFuture<Void> produceAsync(String topic, ThrowoutConsumer<OutputStream, IOException> callback) throws NoConnectionsException {
        return produceAsync(topic, 0, callback);
    }


    //

    public NSQProducer addAddress(String host, int port) {
        addresses.add(new ServerAddress(host, port));
        return this;
    }

    public NSQProducer addAddresses(Collection<ServerAddress> addresses) {
        this.addresses.addAll(addresses);
        return this;
    }

    public NSQProducer removeAddress(String host, int port) {
        addresses.remove(new ServerAddress(host, port));
        return this;
    }

    public NSQProducer setPoolConfig(GenericKeyedObjectPoolConfig<Connection> poolConfig) {
        if (!started) {
            this.poolConfig = poolConfig;
        }
        return this;
    }


    private void checkErrorFrame(NSQFrame frame) throws BadMessageException, BadTopicException {
        if (frame instanceof ErrorFrame) {
            String err = ((ErrorFrame) frame).getErrorMessage();
            if (err.startsWith("E_BAD_TOPIC")) {
                throw new BadTopicException(err);
            }
            if (err.startsWith("E_BAD_MESSAGE")) {
                throw new BadMessageException(err);
            }
        }
    }

    public synchronized NSQProducer setConfig(NSQConfig config) {
        if (!started) {
            this.config = config.clone();
        }
        return this;
    }

    public synchronized void shutdown() {
        started = false;
        pool.close();
    }
}

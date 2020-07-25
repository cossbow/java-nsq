package com.github.cossbow.nsq;

import com.github.cossbow.nsq.exceptions.NSQException;
import com.github.cossbow.nsq.exceptions.NoConnectionsException;
import com.github.cossbow.nsq.frames.ErrorFrame;
import com.github.cossbow.nsq.frames.MessageFrame;
import com.github.cossbow.nsq.frames.NSQFrame;
import com.github.cossbow.nsq.frames.ResponseFrame;
import com.github.cossbow.nsq.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


public class Connection {
    private static final Logger log = LoggerFactory.getLogger(Connection.class);

    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<Connection> STATE = AttributeKey.valueOf("Connection.state");
    private static EventLoopGroup defaultGroup;

    static final int AvailableGroupType_Nio = 1;
    static final int AvailableNioType_Epoll = 2;
    static final int AvailableGroupType_KQueue = 3;
    private static final int AvailableGroupType;
    private static final Class<? extends Channel> SocketChannelClass;

    static {
        if (isEpollIsAvailable()) {
            AvailableGroupType = AvailableNioType_Epoll;
            SocketChannelClass = EpollSocketChannel.class;
        } else if (isKQueueAvailable()) {
            AvailableGroupType = AvailableGroupType_KQueue;
            SocketChannelClass = KQueueSocketChannel.class;
        } else {
            AvailableGroupType = AvailableGroupType_Nio;
            SocketChannelClass = NioSocketChannel.class;
        }
    }


    private final ServerAddress address;
    private final Channel channel;
    private NSQConsumer consumer;
    private Consumer<NSQException> errorCallback = null;
    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);

    private final NSQConfig config;

    public static final long HEARTBEAT_MAX_INTERVAL = 60L * 1000L;//default one minute
    private volatile AtomicReference<Long> lastHeartbeatSuccess = new AtomicReference<Long>(System.currentTimeMillis());


    public Connection(final ServerAddress serverAddress, final NSQConfig config) throws NoConnectionsException, InterruptedException {
        this.address = serverAddress;
        this.config = config;

        final var bootstrap = new Bootstrap();
        var group = config.getEventLoopGroup(getDefaultGroup());
        bootstrap.group(group);
        bootstrap.channel(SocketChannelClass);
        bootstrap.handler(new NSQClientInitializer());
        // Start the connection attempt.
        final ChannelFuture future = bootstrap.connect(new InetSocketAddress(serverAddress.getHost(),
                serverAddress.getPort()));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        log.info("Created connection: " + serverAddress.toString());
        this.channel.attr(STATE).set(this);
        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        channel.write(buf);
        channel.flush();

        //identify
        final NSQCommand identify = NSQCommand.identify(config::write);

        try {
            final NSQFrame response = commandAndWait(identify);
            if (response != null) {
                log.info("Server identification: " + ((ResponseFrame) response).getMessage());
            }
        } catch (final TimeoutException e) {
            log.error("Creating connection timed out", e);
            close();
        } catch (InterruptedException e) {
            close();
            throw e;
        }
    }

    private static EventLoopGroup getDefaultGroup() {
        synchronized (MAGIC_PROTOCOL_VERSION) {
            if (defaultGroup == null) {
                var nt = System.getProperty("com.github.cossbow.nsq.Connection.EventLoopThreads");
                int n = 1;
                if (null != nt) {
                    try {
                        n = Integer.parseInt(nt);
                    } catch (NumberFormatException e) {
                        log.error("EventThreads must be an integer");
                    }
                    if (n < 1) {
                        log.error("EventThreads must be positive");
                        n = 1;
                    }
                }

                var s = System.getSecurityManager();
                var group = (s != null) ? s.getThreadGroup() :
                        Thread.currentThread().getThreadGroup();
                var threadNumber = new AtomicInteger(1);
                ThreadFactory tf = r -> {
                    var t = new Thread(group, r, "com.github.cossbow.nsq-group-" + threadNumber.getAndIncrement());
                    if (t.isDaemon())
                        t.setDaemon(false);
                    if (t.getPriority() != Thread.NORM_PRIORITY)
                        t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                };
                switch (AvailableGroupType) {
                    case AvailableNioType_Epoll:
                        defaultGroup = new EpollEventLoopGroup(n, tf);
                        break;
                    case AvailableGroupType_KQueue:
                        defaultGroup = new KQueueEventLoopGroup(n, tf);
                        break;
                    default:
                        defaultGroup = new NioEventLoopGroup(n, tf);
                        break;
                }
            }
            return defaultGroup;
        }
    }

    public boolean isConnected() {
        return channel.isActive();
    }

    public boolean isHeartbeatStatusOK() {
        if (System.currentTimeMillis() - lastHeartbeatSuccess.get() > HEARTBEAT_MAX_INTERVAL) {
            return false;
        }
        return true;
    }

    public void incoming(final NSQFrame frame) throws InterruptedException {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
                heartbeat();
                return;
            } else {
                if (!requests.isEmpty()) {
                    try {
                        responses.offer(frame, 20, TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                        log.info("Thread was interrupted, probably shutting down", e);
                        close();
                        throw e;
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            if (errorCallback != null) {
                errorCallback.accept(NSQException.of((ErrorFrame) frame));
            }
            try {
                responses.add(frame);
            } catch (IllegalStateException e) {
                log.warn("response queue full");
            }
            return;
        }

        if (frame instanceof MessageFrame) {
            final MessageFrame msg = (MessageFrame) frame;

            final var message = new NSQMessage(this);
            message.setAttempts(msg.getAttempts());
            message.setId(msg.getId());
            message.setTimestamp(msg.getTimestamp());
            message.setCompress(msg.getCompress());
            message.setBuf(msg.getBuf());
            message.setNanoseconds(msg.getTimestamp());
            consumer.processMessage(message);
            return;
        }
        log.warn("Unknown frame type: " + frame);
    }


    private void heartbeat() {
        log.trace("HEARTBEAT!");
        command(NSQCommand.nop());
        lastHeartbeatSuccess.getAndSet(System.currentTimeMillis());
    }

    public void setErrorCallback(final Consumer<NSQException> callback) {
        errorCallback = callback;
    }

    public void close() {
        log.info("Closing  connection: " + this);
        channel.disconnect();
    }

    public NSQFrame commandAndWait(final NSQCommand command) throws TimeoutException, InterruptedException {
        try {
            if (!requests.offer(command, 15, TimeUnit.SECONDS)) {
                command.release();
                throw new TimeoutException("command: " + command + " timeout");
            }

            responses.clear(); //clear the response queue if needed.
            final ChannelFuture future = command(command);

            if (!future.await(15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timeout");
            }

            final NSQFrame frame = responses.poll(15, TimeUnit.SECONDS);
            if (frame == null) {
                throw new TimeoutException("command: " + command + " timeout");
            }

            requests.poll(); //clear the request object
            return frame;
        } catch (final InterruptedException e) {
            close();
            log.info("Thread was interrupted, maybe shutting down");
            throw e;
        }
    }

    public ChannelFuture command(final NSQCommand command) {
        try {
            return channel.writeAndFlush(command);
        } catch (Throwable e) {
            command.release();
            throw e;
        }
    }

    public ServerAddress getServerAddress() {
        return address;
    }

    public NSQConfig getConfig() {
        return config;
    }

    public void setConsumer(final NSQConsumer consumer) {
        this.consumer = consumer;
    }


    //

    private static boolean isEpollIsAvailable() {
        try {
            Class.forName("io.netty.channel.epoll.Epoll");
            return Epoll.isAvailable();
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    private static boolean isKQueueAvailable() {
        try {
            Class.forName("io.netty.channel.kqueue.KQueue");
            return KQueue.isAvailable();
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }
}

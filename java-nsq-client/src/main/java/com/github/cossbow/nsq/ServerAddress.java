package com.github.cossbow.nsq;

import java.util.Objects;

public class ServerAddress {

    public ServerAddress(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    public ServerAddress(final String host, final int port, final int httpPort) {
        this.host = host;
        this.port = port;
        this.httpPort = httpPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String httpAddress() {
        return "http://" + host + ':' + httpPort;
    }

    public String toString() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ServerAddress)) return false;
        final ServerAddress that = (ServerAddress) o;
        return (port == that.port) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    private String host;
    private int port;
    private int httpPort;
}

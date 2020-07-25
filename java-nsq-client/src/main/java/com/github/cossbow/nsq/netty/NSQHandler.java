package com.github.cossbow.nsq.netty;

import com.github.cossbow.nsq.Connection;
import com.github.cossbow.nsq.frames.NSQFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {
    private final static Logger log= LoggerFactory.getLogger(NSQHandler.class);

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Connection connection = ctx.channel().attr(Connection.STATE).get();
        if (connection != null) {
            log.info("Channel disconnected! " + connection);
        } else {
            log.error("No connection set for : " + ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        log.error("NSQHandler exception caught", cause);

        ctx.channel().close();
        Connection con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            con.close();
        } else {
            log.warn("No connection set for : " + ctx.channel());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) throws Exception {
        final Connection con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            ctx.channel().eventLoop().execute(() -> {
                try {
                    con.incoming(msg);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        } else {
            log.warn("No connection set for : " + ctx.channel());
        }
    }
}

package com.azhe.netty.test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;


/**
 * @author linzherong
 * @date 2026/8/10 19:30
 */
public class MyServer {

    public static void main(String[] args) {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(8);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)   // 服务类型
                .option(ChannelOption.SO_BACKLOG, 128) // 服务默认参数
                .handler(new LoggingHandler(LogLevel.INFO))  // 服务端处理
                .childOption(ChannelOption.SO_KEEPALIVE, true)  // 客户端类型
//                .childHandler(new ServerChannelInitializerHttpDemo());
                .childHandler(new ServerChannelInitializerSend());

            bootstrap.bind("localhost", 8081).sync().channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Gracefully release both event loop groups when the server stops.
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }

    }

    /**
     * 服务端接收处理
     */
    private static class ServerChannelInitializerHttpDemo extends ChannelInitializer<NioSocketChannel> {
        @Override
        protected void initChannel(NioSocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpServerCodec());  // http 编码
            p.addLast(new HttpServerExpectContinueHandler());
            p.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    System.out.println("receive：" + msg);
                    super.channelRead(ctx, msg);
                }
            });
        }
    }

    private static class ServerChannelInitializerSend extends ChannelInitializer<NioSocketChannel> {

        @Override
        protected void initChannel(NioSocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new LineBasedFrameDecoder(1024));
            p.addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

                    ByteBuf buf = (ByteBuf)msg;
                    String request = buf.toString(CharsetUtil.UTF_8);

                    System.out.println("RECEIVE MSG:["+request+"]");

                    String response = "已收到【"+request+"】ack" + System.getProperty("line.separator");

                    ctx.writeAndFlush(Unpooled.copiedBuffer(response.getBytes()));
                }
            });
        }
    }


}

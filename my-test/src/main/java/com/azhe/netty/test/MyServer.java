package com.azhe.netty.test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.nio.channels.Selector;

/**
 * @author linzherong
 * @date 2026/8/10 19:30
 */
public class MyServer {

    public static void main(String[] args) {
        EventLoopGroup boss = new NioEventLoopGroup(2);
        EventLoopGroup worker = new NioEventLoopGroup(8);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)   // 服务类型
                .option(ChannelOption.SO_BACKLOG, 128) // 服务默认参数
                .handler(new LoggingHandler(LogLevel.INFO))  // 服务端处理
                .childOption(ChannelOption.SO_KEEPALIVE, true)  // 客户端类型
                .childHandler(new ChannelInitializer<SocketChannel>() {   // 有客户端连接后，对应的连接fd处理
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec())     // http 编解码
                            .addLast(new HttpServerExpectContinueHandler())    // http 编解码
                            .addLast(new ChannelInboundHandlerAdapter() {   // 自定义处理
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                    System.out.println("receive：" + msg);
                                    super.channelRead(ctx, msg);
                                }
                            });
                    }
                });

            ChannelFuture localhost = bootstrap.bind("localhost", 8081).sync().channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Gracefully release both event loop groups when the server stops.
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }

    }

}

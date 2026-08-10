package com.azhe.netty.test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Connects to {@link MyServer} and sends one HTTP request.
 */
public class MyClient {

    /**
     * Starts the client, sends a GET request, and closes the connection.
     *
     * @param args command-line arguments
     * @throws Exception if the client cannot connect or send the request
     */
    public static void main(String[] args) throws Exception {
        EventLoopGroup worker = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap()
                .group(worker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                            .addLast(new LoggingHandler(LogLevel.INFO))
                            .addLast(new HttpClientCodec())
                            .addLast(new SimpleChannelInboundHandler<HttpObject>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext context,
                                                            HttpObject message) {
                                    if (message instanceof HttpResponse) {
                                        System.out.println("response: " + message);
                                    }
                                }
                            });
                    }
                });

            // Connect to the address bound by MyServer.
            Channel channel = bootstrap.connect("localhost", 8081).sync().channel();

            // Build an HTTP/1.1 request that the server's HttpServerCodec can decode.
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            request.headers()
                .set(HttpHeaderNames.HOST, "localhost:8081")
                .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, 0);

            // Flush the request before closing so MyServer receives it.
            channel.writeAndFlush(request).sync();
            channel.close().sync();
        } finally {
            // Release the client event loop after the request completes.
            worker.shutdownGracefully().sync();
        }
    }
}

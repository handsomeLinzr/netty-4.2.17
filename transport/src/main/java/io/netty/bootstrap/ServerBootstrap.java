/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * {@link Bootstrap} sub-class which allows easy bootstrap of {@link ServerChannel}
 *
 */
public class ServerBootstrap extends AbstractBootstrap<ServerBootstrap, ServerChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ServerBootstrap.class);

    // 通过 .childOption 设置进来的参数
    // The order in which child ChannelOptions are applied is important they may depend on each other for validation
    // purposes.
    private final Map<ChannelOption<?>, Object> childOptions = new LinkedHashMap<ChannelOption<?>, Object>();
    private final Map<AttributeKey<?>, Object> childAttrs = new ConcurrentHashMap<AttributeKey<?>, Object>();

    // 对应当前 serverBootstrap 的 config
    private final ServerBootstrapConfig config = new ServerBootstrapConfig(this);

    // 客户端连接后的处理的线程组
    private volatile EventLoopGroup childGroup;

    // 客户端连接建立后的处理器
    private volatile ChannelHandler childHandler;

    public ServerBootstrap() { }

    private ServerBootstrap(ServerBootstrap bootstrap) {
        super(bootstrap);
        childGroup = bootstrap.childGroup;
        childHandler = bootstrap.childHandler;
        synchronized (bootstrap.childOptions) {
            childOptions.putAll(bootstrap.childOptions);
        }
        childAttrs.putAll(bootstrap.childAttrs);
    }

    /**
     * Specify the {@link EventLoopGroup} which is used for the parent (acceptor) and the child (client).
     *
     * 设置
     */
    @Override
    public ServerBootstrap group(EventLoopGroup group) {
        return group(group, group);
    }

    /**
     * Set the {@link EventLoopGroup} for the parent (acceptor) and the child (client). These
     * {@link EventLoopGroup}'s are used to handle all the events and IO for {@link ServerChannel} and
     * {@link Channel}'s.
     * <p>
     * <strong>Important:</strong> Usually this is only useful for advanced use-cases and usually
     * {@link #group(EventLoopGroup)} is the preferred way to configure the group.
     *
     * 设置接受连接的线程组 和 任务处理的 线程组
     *
     */
    public ServerBootstrap group(EventLoopGroup parentGroup, EventLoopGroup childGroup) {
        // 设置 group
        super.group(parentGroup);
        if (this.childGroup != null) {
            throw new IllegalStateException("childGroup set already");
        }

        // 设置 childGroup
        this.childGroup = ObjectUtil.checkNotNull(childGroup, "childGroup");
        return this;
    }

    /**
     * Allow to specify a {@link ChannelOption} which is used for the {@link Channel} instances once they get created
     * (after the acceptor accepted the {@link Channel}). Use a value of {@code null} to remove a previous set
     * {@link ChannelOption}.
     *
     *
     * 连接的 channel 的设置配置
     *
     */
    public <T> ServerBootstrap childOption(ChannelOption<T> childOption, T value) {
        ObjectUtil.checkNotNull(childOption, "childOption");
        synchronized (childOptions) {
            if (value == null) {
                childOptions.remove(childOption);
            } else {
                // 设置配置
                childOptions.put(childOption, value);
            }
        }
        return this;
    }

    /**
     * Set the specific {@link AttributeKey} with the given value on every child {@link Channel}. If the value is
     * {@code null} the {@link AttributeKey} is removed
     */
    public <T> ServerBootstrap childAttr(AttributeKey<T> childKey, T value) {
        ObjectUtil.checkNotNull(childKey, "childKey");
        if (value == null) {
            childAttrs.remove(childKey);
        } else {
            childAttrs.put(childKey, value);
        }
        return this;
    }

    /**
     * Set the {@link ChannelHandler} which is used to serve the request for the {@link Channel}'s.
     */
    public ServerBootstrap childHandler(ChannelHandler childHandler) {
        this.childHandler = ObjectUtil.checkNotNull(childHandler, "childHandler");
        return this;
    }

    /**
     * 初始化服务端通道
     *
     * 然后给当前 pipeline 添加一个初始化监听器
     *
     * @param channel
     * @throws Throwable
     */
    @Override
    void init(Channel channel) throws Throwable {

        // 设置服务端 channel 的options属性配置
        setChannelOptions(channel, newOptionsArray(), logger);

        // 设置服务端 channel 的attr 属性配置
        setAttributes(channel, newAttributesArray());

        // 获取 channel 的 pipeline 通道
        ChannelPipeline p = channel.pipeline();

        // 获取 worker 事件组
        final EventLoopGroup currentChildGroup = childGroup;

        // 获取连接后的客户端处理器
        final ChannelHandler currentChildHandler = childHandler;

        // 客户端连接的参数
        final Entry<ChannelOption<?>, Object>[] currentChildOptions = newOptionsArray(childOptions);
        final Entry<AttributeKey<?>, Object>[] currentChildAttrs = newAttributesArray(childAttrs);

        // 获取初始化的扩展
        final Collection<ChannelInitializerExtension> extensions = getInitializerExtensions();

        /**
         * ChannelInitializer 一次性初始化 handler
         * 负责添加一个 ServerBootstrapAcceptor 处理器，添加完后，自己就移除了
         * ServerBootstrapAcceptor 主要负责接收到客户端连接后，对连接的初始化操作
         * 初始化好了，就需要移除，因为后边的读写不需要用到了
         *
         */
        p.addLast(new ChannelInitializer<Channel>() {

            // 在调用 register 中，会被调用到
            @Override
            public void initChannel(final Channel ch) {

                // 获取当前的 pipeline
                final ChannelPipeline pipeline = ch.pipeline();

                // 获取自定义的 server 处理器
                ChannelHandler handler = config.handler();
                if (handler != null) {
                    // 添加到 pipeline
                    pipeline.addLast(handler);
                }

                // 线程组线程执行
                // 在 pipeline 最后添加 ServerBootstrapAcceptor 处理
                // 所以现在 pipeline 就要有 上边的 handler + ServerBootstrapAcceptor
                ch.eventLoop().execute(new Runnable() {
                    @Override
                    public void run() {
                        pipeline.addLast(new ServerBootstrapAcceptor(
                                    // worker           childHandler         childOptions          childAttrs
                                ch, currentChildGroup, currentChildHandler, currentChildOptions, currentChildAttrs,
                                extensions));
                    }
                });
            }
        });
        if (!extensions.isEmpty() && channel instanceof ServerChannel) {
            ServerChannel serverChannel = (ServerChannel) channel;
            for (ChannelInitializerExtension extension : extensions) {
                try {
                    extension.postInitializeServerListenerChannel(serverChannel);
                } catch (Exception e) {
                    logger.warn("Exception thrown from postInitializeServerListenerChannel", e);
                }
            }
        }
    }

    @Override
    public ServerBootstrap validate() {
        super.validate();
        if (childHandler == null) {
            throw new IllegalStateException("childHandler not set");
        }
        if (childGroup == null) {
            logger.warn("childGroup is not set. Using parentGroup instead.");
            childGroup = config.group();
        }
        return this;
    }

    private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {

        private final EventLoopGroup childGroup;
        private final ChannelHandler childHandler;
        private final Entry<ChannelOption<?>, Object>[] childOptions;
        private final Entry<AttributeKey<?>, Object>[] childAttrs;
        private final Runnable enableAutoReadTask;
        private final Collection<ChannelInitializerExtension> extensions;

        // 接收请求的处理器
        ServerBootstrapAcceptor(
                final Channel channel, EventLoopGroup childGroup, ChannelHandler childHandler,
                Entry<ChannelOption<?>, Object>[] childOptions, Entry<AttributeKey<?>, Object>[] childAttrs,
                Collection<ChannelInitializerExtension> extensions) {
            this.childGroup = childGroup;
            this.childHandler = childHandler;
            this.childOptions = childOptions;
            this.childAttrs = childAttrs;
            this.extensions = extensions;

            // Task which is scheduled to re-enable auto-read.
            // It's important to create this Runnable before we try to submit it as otherwise the URLClassLoader may
            // not be able to load the class because of the file limit it already reached.
            //
            // See https://github.com/netty/netty/issues/1328
            enableAutoReadTask = new Runnable() {
                @Override
                public void run() {
                    channel.config().setAutoRead(true);
                }
            };
        }

        /**
         * 在客户端通道刚连接后，调用 head.fireChannelRead，就会调用到这里来
         * @param ctx
         * @param msg
         */
        @Override
        @SuppressWarnings("unchecked")
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            // NioSocketChannel 客户端连接
            final Channel child = (Channel) msg;

            // 给客户端的连接加上处理器 childHandler
            child.pipeline().addLast(childHandler);

            try {
                // 设置 options 参数
                setChannelOptions(child, childOptions, logger);
            } catch (Throwable cause) {
                forceClose(child, cause);
                return;
            }
            // 设置 attributes
            setAttributes(child, childAttrs);

            // 扩张，一般是默认是空的，跳过
            if (!extensions.isEmpty()) {
                for (ChannelInitializerExtension extension : extensions) {
                    try {
                        extension.postInitializeServerChildChannel(child);
                    } catch (Exception e) {
                        logger.warn("Exception thrown from postInitializeServerChildChannel", e);
                    }
                }
            }

            try {
                // 调用 childGroup（也就是 worker 事件循环组）注册 child
                // 并添加监听，如果失败了，则强制关闭
                childGroup.register(child).addListener(future -> {
                    if (!future.isSuccess()) {
                        forceClose(child, future.cause());
                    }
                });
            } catch (Throwable t) {
                forceClose(child, t);
            }
        }

        private static void forceClose(Channel child, Throwable t) {
            child.unsafe().closeForcibly();
            logger.warn("Failed to register an accepted channel: {}", child, t);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final ChannelConfig config = ctx.channel().config();
            if (config.isAutoRead()) {
                // stop accept new connections for 1 second to allow the channel to recover
                // See https://github.com/netty/netty/issues/1328
                config.setAutoRead(false);
                ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
            }
            // still let the exceptionCaught event flow through the pipeline to give the user
            // a chance to do something with it
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public ServerBootstrap clone() {
        return new ServerBootstrap(this);
    }

    /**
     * Return the configured {@link EventLoopGroup} which will be used for the child channels or {@code null}
     * if non is configured yet.
     *
     * @deprecated Use {@link #config()} instead.
     */
    @Deprecated
    public EventLoopGroup childGroup() {
        return childGroup;
    }

    final ChannelHandler childHandler() {
        return childHandler;
    }

    final Map<ChannelOption<?>, Object> childOptions() {
        synchronized (childOptions) {
            return copiedMap(childOptions);
        }
    }

    final Map<AttributeKey<?>, Object> childAttrs() {
        return copiedMap(childAttrs);
    }

    @Override
    public final ServerBootstrapConfig config() {
        return config;
    }
}

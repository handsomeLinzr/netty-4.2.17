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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.resolver.NameResolver;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

/**
 * A {@link Bootstrap} that makes it easy to bootstrap a {@link Channel} to use
 * for clients.
 *
 * <p>The {@link #bind()} methods are useful in combination with connectionless transports such as datagram (UDP).
 * For regular TCP connections, please use the provided {@link #connect()} methods.</p>
 */
public class Bootstrap extends AbstractBootstrap<Bootstrap, Channel> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Bootstrap.class);

    private final BootstrapConfig config = new BootstrapConfig(this);

    private ExternalAddressResolver externalResolver;

    // false
    private volatile boolean disableResolver;
    private volatile SocketAddress remoteAddress;

    public Bootstrap() { }

    private Bootstrap(Bootstrap bootstrap) {
        super(bootstrap);
        externalResolver = bootstrap.externalResolver;
        disableResolver = bootstrap.disableResolver;
        remoteAddress = bootstrap.remoteAddress;
    }

    /**
     * Sets the {@link NameResolver} which will resolve the address of the unresolved named address.
     *
     * @param resolver the {@link NameResolver} for this {@code Bootstrap}; may be {@code null}, in which case a default
     *                 resolver will be used
     *
     * @see io.netty.resolver.DefaultAddressResolverGroup
     */
    public Bootstrap resolver(AddressResolverGroup<?> resolver) {
        externalResolver = resolver == null ? null : new ExternalAddressResolver(resolver);
        disableResolver = false;
        return this;
    }

    /**
     * Disables address name resolution. Name resolution may be re-enabled with
     * {@link Bootstrap#resolver(AddressResolverGroup)}
     */
    public Bootstrap disableResolver() {
        externalResolver = null;
        disableResolver = true;
        return this;
    }

    /**
     * The {@link SocketAddress} to connect to once the {@link #connect()} method
     * is called.
     */
    public Bootstrap remoteAddress(SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
        return this;
    }

    /**
     * @see #remoteAddress(SocketAddress)
     */
    public Bootstrap remoteAddress(String inetHost, int inetPort) {
        remoteAddress = InetSocketAddress.createUnresolved(inetHost, inetPort);
        return this;
    }

    /**
     * @see #remoteAddress(SocketAddress)
     */
    public Bootstrap remoteAddress(InetAddress inetHost, int inetPort) {
        remoteAddress = new InetSocketAddress(inetHost, inetPort);
        return this;
    }

    /**
     * Connect a {@link Channel} to the remote peer.
     */
    public ChannelFuture connect() {
        validate();
        SocketAddress remoteAddress = this.remoteAddress;
        if (remoteAddress == null) {
            throw new IllegalStateException("remoteAddress not set");
        }

        return doResolveAndConnect(remoteAddress, config.localAddress());
    }

    /**
     *
     * 创建一个连接连向远程
     *
     * Connect a {@link Channel} to the remote peer.
     */
    public ChannelFuture connect(String inetHost, int inetPort) {
        // 连接
        return connect(InetSocketAddress.createUnresolved(inetHost, inetPort));
    }

    /**
     * Connect a {@link Channel} to the remote peer.
     */
    public ChannelFuture connect(InetAddress inetHost, int inetPort) {
        return connect(new InetSocketAddress(inetHost, inetPort));
    }

    /**
     *
     * 创建连接
     *
     * Connect a {@link Channel} to the remote peer.
     */
    public ChannelFuture connect(SocketAddress remoteAddress) {
        // 校验
        ObjectUtil.checkNotNull(remoteAddress, "remoteAddress");
        validate();

        // 连接
        // config.localAddress() 默认空
        return doResolveAndConnect(remoteAddress, config.localAddress());
    }

    /**
     * Connect a {@link Channel} to the remote peer.
     */
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        ObjectUtil.checkNotNull(remoteAddress, "remoteAddress");
        validate();
        return doResolveAndConnect(remoteAddress, localAddress);
    }

    /**
     * @see #connect()
     */
    private ChannelFuture doResolveAndConnect(final SocketAddress remoteAddress, final SocketAddress localAddress) {
        // 初始化并注册
        final ChannelFuture regFuture = initAndRegister();
        // 获取客户端连接, NioSocketChannel
        final Channel channel = regFuture.channel();

        // regFuture 判断是否已经完成
        // 因为上边的 initAndRegister 是异步的，交给 eventLoop 处理
        // 所以需要判断是否完成
        if (regFuture.isDone()) {
            // 完成了，但是失败了，则返回 regFuture
            if (!regFuture.isSuccess()) {
                return regFuture;
            }
            // 如果成功了，调用 doResolveAndConnect0 继续往下走
            return doResolveAndConnect0(channel, remoteAddress, localAddress, channel.newPromise());
        } else {

            // 还未完成，添加监听器，然后返回
            // Registration future is almost always fulfilled already, but just in case it's not.
            final PendingRegistrationPromise promise = new PendingRegistrationPromise(channel);
            regFuture.addListener(future -> {
                // Directly obtain the cause and do a null check so we only need one volatile read in case of a
                // failure.
                Throwable cause = future.cause();
                if (cause != null) {
                    // Registration on the EventLoop failed so fail the ChannelPromise directly to not cause an
                    // IllegalStateException once we try to access the EventLoop of the Channel.
                    promise.setFailure(cause);
                } else {
                    // Registration was successful, so set the correct executor to use.
                    // See https://github.com/netty/netty/issues/2586
                    promise.registered();
                    doResolveAndConnect0(channel, remoteAddress, localAddress, promise);
                }
            });
            return promise;
        }
    }

    private ChannelFuture doResolveAndConnect0(final Channel channel, SocketAddress remoteAddress,
                                               final SocketAddress localAddress, final ChannelPromise promise) {
        try {
            if (disableResolver) {
                doConnect(remoteAddress, localAddress, promise);
                return promise;
            }

            // 获取当前的线程
            final EventLoop eventLoop = channel.eventLoop();
            AddressResolver<SocketAddress> resolver;
            try {  // DefaultAddressResolverGroup.INSTANCE
                resolver = ExternalAddressResolver.getOrDefault(externalResolver).getResolver(eventLoop);
            } catch (Throwable cause) {
                channel.close();
                return promise.setFailure(cause);
            }

            if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
                // Resolver has no idea about what to do with the specified remote address or it's resolved already.
                doConnect(remoteAddress, localAddress, promise);
                return promise;
            }

            final Future<SocketAddress> resolveFuture = resolver.resolve(remoteAddress);

            if (resolveFuture.isDone()) {
                final Throwable resolveFailureCause = resolveFuture.cause();

                if (resolveFailureCause != null) {
                    // Failed to resolve immediately
                    channel.close();
                    promise.setFailure(resolveFailureCause);
                } else {
                    // 开始连接
                    // Succeeded to resolve immediately; cached? (or did a blocking lookup)
                    doConnect(resolveFuture.getNow(), localAddress, promise);
                }
                return promise;
            }

            // Wait until the name resolution is finished.
            resolveFuture.addListener((FutureListener<SocketAddress>) future -> {
                if (future.cause() != null) {
                    channel.close();
                    promise.setFailure(future.cause());
                } else {
                    doConnect(future.getNow(), localAddress, promise);
                }
            });
        } catch (Throwable cause) {
            promise.tryFailure(cause);
        }
        return promise;
    }

    private static void doConnect(
            final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise connectPromise) {

        // This method is invoked before channelRegistered() is triggered.  Give user handlers a chance to set up
        // the pipeline in its channelRegistered() implementation.
        final Channel channel = connectPromise.channel();
        channel.eventLoop().execute(new Runnable() {
            @Override
            public void run() {
                if (localAddress == null) {

                    // 连接
                    channel.connect(remoteAddress, connectPromise);
                } else {
                    channel.connect(remoteAddress, localAddress, connectPromise);
                }

                // 添加监听
                connectPromise.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            }
        });
    }

    /**
     *
     * 客户端初始化
     * 1.给 pipeline 添加自定义 handler
     * 2.设置 options 参数
     * 3.设置 attributes 参数
     *
     * @param channel
     * @throws Throwable
     */
    @Override
    void init(Channel channel) throws Throwable {

        // DefaultChannelPipeline
        ChannelPipeline p = channel.pipeline();
        // 添加前边自定义的 handler
        p.addLast(config.handler());

        // options 配置
        setChannelOptions(channel, newOptionsArray(), logger);

        // attributes 参数配置
        setAttributes(channel, newAttributesArray());

        // 获取扩展，默认空
        Collection<ChannelInitializerExtension> extensions = getInitializerExtensions();
        if (!extensions.isEmpty()) {
            for (ChannelInitializerExtension extension : extensions) {
                try {
                    extension.postInitializeClientChannel(channel);
                } catch (Exception e) {
                    logger.warn("Exception thrown from postInitializeClientChannel", e);
                }
            }
        }
    }

    @Override
    public Bootstrap validate() {
        super.validate();
        if (config.handler() == null) {
            throw new IllegalStateException("handler not set");
        }
        return this;
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public Bootstrap clone() {
        return new Bootstrap(this);
    }

    /**
     * Returns a deep clone of this bootstrap which has the identical configuration except that it uses
     * the given {@link EventLoopGroup}. This method is useful when making multiple {@link Channel}s with similar
     * settings.
     */
    public Bootstrap clone(EventLoopGroup group) {
        Bootstrap bs = new Bootstrap(this);
        bs.group = group;
        return bs;
    }

    @Override
    public final BootstrapConfig config() {
        return config;
    }

    final SocketAddress remoteAddress() {
        return remoteAddress;
    }

    final AddressResolverGroup<?> resolver() {
        if (disableResolver) {
            return null;
        }
        return ExternalAddressResolver.getOrDefault(externalResolver);
    }

    /* Holder to avoid NoClassDefFoundError in case netty-resolver dependency is excluded
       (e.g. some address families do not need name resolution) */
    static final class ExternalAddressResolver {
        final AddressResolverGroup<SocketAddress> resolverGroup;

        @SuppressWarnings("unchecked")
        ExternalAddressResolver(AddressResolverGroup<?> resolverGroup) {
            this.resolverGroup = (AddressResolverGroup<SocketAddress>) resolverGroup;
        }

        @SuppressWarnings("unchecked")
        static AddressResolverGroup<SocketAddress> getOrDefault(ExternalAddressResolver externalResolver) {
            if (externalResolver == null) {
                AddressResolverGroup<?> defaultResolverGroup = DefaultAddressResolverGroup.INSTANCE;
                return (AddressResolverGroup<SocketAddress>) defaultResolverGroup;
            }
            return externalResolver.resolverGroup;
        }
    }
}

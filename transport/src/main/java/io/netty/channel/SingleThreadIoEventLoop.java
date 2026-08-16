/*
 * Copyright 2024 The Netty Project
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
package io.netty.channel;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link IoEventLoop} implementation that execute all its submitted tasks in a single thread using the provided
 * {@link IoHandler}.
 */
public class SingleThreadIoEventLoop extends SingleThreadEventLoop implements IoEventLoop {

    // TODO: Is this a sensible default ?    1000 ms
    private static final long DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS = TimeUnit.MILLISECONDS.toNanos(Math.max(100,
            SystemPropertyUtil.getInt("io.netty.eventLoop.maxTaskProcessingQuantumMs", 1000)));

    // 默认 1000 ms
    private final long maxTaskProcessingQuantumNs;
    private final IoHandlerContext context = new IoHandlerContext() {
        @Override
        public boolean canBlock() {
            assert inEventLoop();
            return !hasTasks() && !hasScheduledTasks();
        }

        @Override
        public long delayNanos(long currentTimeNanos) {
            assert inEventLoop();
            return SingleThreadIoEventLoop.this.delayNanos(currentTimeNanos);
        }

        @Override
        public long deadlineNanos() {
            assert inEventLoop();
            return SingleThreadIoEventLoop.this.deadlineNanos();
        }

        @Override
        public void reportActiveIoTime(long activeNanos) {
            SingleThreadIoEventLoop.this.reportActiveIoTime(activeNanos);
        }

        @Override
        public boolean shouldReportActiveIoTime() {
            return isSuspensionSupported();
        }
    };

    // 创建对应的 NioIoHandler，包装了 selector 选择器
    private final IoHandler ioHandler;

    // 注册数量，在 registerForIo0 方法中会自增
    private final AtomicInteger numRegistrations = new AtomicInteger();

    /**
     *  Creates a new instance
     *
     * @param parent            the parent that holds this {@link IoEventLoop}.
     * @param threadFactory     the {@link ThreadFactory} that is used to create the underlying {@link Thread}.
     * @param ioHandlerFactory  the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler} to
     *                          handle IO.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, ThreadFactory threadFactory,
                                   IoHandlerFactory ioHandlerFactory) {
        super(parent, threadFactory, false,
                ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported());
        this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
        this.ioHandler = ioHandlerFactory.newHandler(this);
    }

    /**
     *  Creates a new instance
     *
     * @param parent            the parent that holds this {@link IoEventLoop}.
     * @param executor          the {@link Executor} that is used for dispatching the work.
     * @param ioHandlerFactory  the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler} to
     *                          handle IO.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor, IoHandlerFactory ioHandlerFactory) {
        super(parent, executor, false,
                ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported());
        // 默认 1000 ms
        this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;
        // 创建 NioIoHandler
        this.ioHandler = ioHandlerFactory.newHandler(this);
    }

    /**
     *  Creates a new instance
     *
     * @param parent                        the parent that holds this {@link IoEventLoop}.
     * @param threadFactory                 the {@link ThreadFactory} that is used to create the underlying
     *                                      {@link Thread}.
     * @param ioHandlerFactory              the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler}
     *                                      to handle IO.
     * @param maxPendingTasks               the maximum pending tasks that are allowed before
     *                                      {@link RejectedExecutionHandler#rejected(Runnable,
     *                                          SingleThreadEventExecutor)}
     *                                      is called to handle it.
     * @param rejectedExecutionHandler      the {@link RejectedExecutionHandler} that handles when more tasks are added
     *                                      then allowed per {@code maxPendingTasks}.
     * @param maxTaskProcessingQuantumMs    the maximum number of milliseconds that will be spent to run tasks before
     *                                      trying to run IO again.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, ThreadFactory threadFactory,
                                   IoHandlerFactory ioHandlerFactory, int maxPendingTasks,
                                   RejectedExecutionHandler rejectedExecutionHandler, long maxTaskProcessingQuantumMs) {
        super(parent, threadFactory, false,
                ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported(),
                maxPendingTasks, rejectedExecutionHandler);
        this.maxTaskProcessingQuantumNs =
                ObjectUtil.checkPositiveOrZero(maxTaskProcessingQuantumMs, "maxTaskProcessingQuantumMs") == 0 ?
                        DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS :
                        TimeUnit.MILLISECONDS.toNanos(maxTaskProcessingQuantumMs);
        this.ioHandler = ioHandlerFactory.newHandler(this);
    }

    /**
     *  Creates a new instance
     *
     * @param parent                        the parent that holds this {@link IoEventLoop}.
     * @param ioHandlerFactory              the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler}
     *                                      to handle IO.
     * @param maxPendingTasks               the maximum pending tasks that are allowed before
     *                                      {@link RejectedExecutionHandler#rejected(Runnable,
     *                                          SingleThreadEventExecutor)}
     *                                      is called to handle it.
     * @param rejectedExecutionHandler      the {@link RejectedExecutionHandler} that handles when more tasks are added
     *                                      then allowed per {@code maxPendingTasks}.
     * @param maxTaskProcessingQuantumMs    the maximum number of milliseconds that will be spent to run tasks before
     *                                      trying to run IO again.
     */
    public SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor,
                                   IoHandlerFactory ioHandlerFactory, int maxPendingTasks,
                                   RejectedExecutionHandler rejectedExecutionHandler,
                                   long maxTaskProcessingQuantumMs) {
        super(parent, executor, false,
                ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported(),
                maxPendingTasks, rejectedExecutionHandler);
        this.maxTaskProcessingQuantumNs =
                ObjectUtil.checkPositiveOrZero(maxTaskProcessingQuantumMs, "maxTaskProcessingQuantumMs") == 0 ?
                        DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS :
                        TimeUnit.MILLISECONDS.toNanos(maxTaskProcessingQuantumMs);
        this.ioHandler = ioHandlerFactory.newHandler(this);
    }

    /**
     *
     *  Creates a new instance
     *
     * @param parent                    the parent that holds this {@link IoEventLoop}.
     * @param executor                  the {@link Executor} that is used for dispatching the work.
     * @param ioHandlerFactory          the {@link IoHandlerFactory} that should be used to obtain {@link IoHandler}
     *                                  to handle IO.
     * @param taskQueue                 the {@link Queue} used for storing pending tasks.
     * @param tailTaskQueue             the {@link Queue} used for storing tail pending tasks.
     * @param rejectedExecutionHandler  the {@link RejectedExecutionHandler} that handles when more tasks are added
     *                                  then allowed.
     *
     *
     * 创建一个实例
     *
     */
    protected SingleThreadIoEventLoop(IoEventLoopGroup parent, Executor executor,
                                      IoHandlerFactory ioHandlerFactory, Queue<Runnable> taskQueue,
                                      Queue<Runnable> tailTaskQueue,
                                      RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, false,
                ObjectUtil.checkNotNull(ioHandlerFactory, "ioHandlerFactory").isChangingThreadSupported(),  // true
                taskQueue, tailTaskQueue, rejectedExecutionHandler);

        // DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS =
        this.maxTaskProcessingQuantumNs = DEFAULT_MAX_TASK_PROCESSING_QUANTUM_NS;

        // new NioIoHandler(executor, selectorProvider, selectStrategyFactory.newSelectStrategy());
        // 其中包装了 selector 选择器
        this.ioHandler = ioHandlerFactory.newHandler(this);
    }

    @Override
    protected void run() {
        assert inEventLoop();
        ioHandler.initialize();
        do {
            // io 执行
            runIo();
            if (isShuttingDown()) {
                ioHandler.prepareToDestroy();
            }

            // 执行所有任务
            // Now run all tasks for the maximum configured amount of time before trying to run IO again.
            runAllTasks(maxTaskProcessingQuantumNs);

            // We should continue with our loop until we either confirmed a shutdown or we can suspend it.
        } while (!confirmShutdown() && !canSuspend());
    }

    protected final IoHandler ioHandler() {
        return ioHandler;
    }

    @Override
    protected boolean canSuspend(int state) {
        // We should only allow to suspend if there are no registrations on this loop atm.
        return super.canSuspend(state) && numRegistrations.get() == 0;
    }

    /**
     * Called when IO will be processed for all the {@link IoHandle}s on this {@link SingleThreadIoEventLoop}.
     * This method returns the number of {@link IoHandle}s for which IO was processed.
     *
     * This method must be called from the {@link EventLoop} thread.
     */
    protected int runIo() {
        assert inEventLoop();
        return ioHandler.run(context);
    }

    @Override
    public IoEventLoop next() {
        return this;
    }

    /**
     * server 注册
     * handle 给的是 unsafe
     * @param handle        the {@link IoHandle} to register.
     * @return
     */
    @Override
    public final Future<IoRegistration> register(final IoHandle handle) {
        // 创建一个异步器 DefaultPromise
        Promise<IoRegistration> promise = newPromise();
        if (inEventLoop()) {

            // 当前在线程组中的情况
            // 注册
            // promise 又是一个新的 异步器
            registerForIo0(handle, promise);
        } else {
            // 不在的情况
            execute(() -> registerForIo0(handle, promise));
        }

        // 返回异步器
        return promise;
    }

    @Override
    protected int getNumOfRegisteredChannels() {
        return numRegistrations.get();
    }

    // io 线程注册
    private void registerForIo0(final IoHandle handle, Promise<IoRegistration> promise) {
        assert inEventLoop();
        final IoRegistration registration;
        try {
            /**
             * 注册
             */
            registration = ioHandler.register(handle);
        } catch (Exception e) {
            promise.setFailure(e);
            return;
        }
        // 当前事件组中的注册数量 +1
        numRegistrations.incrementAndGet();
        // 唤醒 promise
        promise.setSuccess(new IoRegistrationWrapper(registration));
    }

    @Override
    protected final void wakeup(boolean inEventLoop) {
        ioHandler.wakeup();
    }

    @Override
    protected final void cleanup() {
        assert inEventLoop();
        ioHandler.destroy();
    }

    @Override
    public boolean isCompatible(Class<? extends IoHandle> handleType) {
        return ioHandler.isCompatible(handleType);
    }

    @Override
    public boolean isIoType(Class<? extends IoHandler> handlerType) {
        return ioHandler.getClass().equals(handlerType);
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    protected static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    private final class IoRegistrationWrapper implements IoRegistration {
        private final IoRegistration registration;
        IoRegistrationWrapper(IoRegistration registration) {
            this.registration = registration;
        }

        @Override
        public <T> T attachment() {
            return registration.attachment();
        }

        @Override
        public long submit(IoOps ops) {
            return registration.submit(ops);
        }

        @Override
        public boolean isValid() {
            return registration.isValid();
        }

        @Override
        public boolean cancel() {
            if (registration.cancel()) {
                numRegistrations.decrementAndGet();
                return true;
            }
            return false;
        }
    }
}

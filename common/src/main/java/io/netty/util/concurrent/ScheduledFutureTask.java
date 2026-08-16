/*
 * Copyright 2013 The Netty Project
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

package io.netty.util.concurrent;

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueueNode;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 定时任务
 */
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class ScheduledFutureTask<V> extends PromiseTask<V> implements ScheduledFuture<V>, PriorityQueueNode {
    // set once when added to priority queue
    private long id;

    // 执行的时间点
    private long deadlineNanos;

    // 0- 无重复    >0 以固定速率重复   <0 -以固定延迟重复
    /* 0 - no repeat, >0 - repeat at fixed rate, <0 - repeat with fixed delay */
    private final long periodNanos;

    private int queueIndex = INDEX_NOT_IN_QUEUE;

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Runnable runnable, long nanoTime) {

        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Runnable runnable, long nanoTime, long period) {

        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime, long period) {

        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime) {

        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    private static long validatePeriod(long period) {
        if (period == 0) {
            throw new IllegalArgumentException("period: 0 (expected: != 0)");
        }
        return period;
    }

    ScheduledFutureTask<V> setId(long id) {
        if (this.id == 0L) {
            this.id = id;
        }
        return this;
    }

    long getId() {
        return id;
    }

    @Override
    protected EventExecutor executor() {
        return super.executor();
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    /**
     * 设置消耗，主要是设置 deadlineNanos 延迟的时间
     */
    void setConsumed() {
        // Optimization to avoid checking system clock again
        // after deadline has passed and task has been dequeued
        if (periodNanos == 0) {

            // 如果 periodNanos 为 0，说明不需要重复，则设置 deadlineNanos = 0
            assert scheduledExecutor().getCurrentTimeNanos() >= deadlineNanos;
            deadlineNanos = 0L;
        }
    }

    /**
     * 获取当前任务的延迟时间
     * @return
     */
    public long delayNanos() {
        if (deadlineNanos == 0L) {
            // 如果已经 deadlineNanos 是 0 了，则说明到时间了，返回 0
            // 这里在 setConsumed 已经设置了
            return 0L;
        }
        // 获取当前时间 和 deadlineNanos 的差值
        return delayNanos(scheduledExecutor().getCurrentTimeNanos());
    }

    static long deadlineToDelayNanos(long currentTimeNanos, long deadlineNanos) {
        return deadlineNanos == 0L ? 0L : Math.max(0L, deadlineNanos - currentTimeNanos);
    }

    public long delayNanos(long currentTimeNanos) {
        return deadlineToDelayNanos(currentTimeNanos, deadlineNanos);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delayNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (this == o) {
            return 0;
        }

        ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) o;
        long d = deadlineNanos() - that.deadlineNanos();
        if (d < 0) {
            return -1;
        } else if (d > 0) {
            return 1;
        } else if (id < that.id) {
            return -1;
        } else {
            assert id != that.id;
            return 1;
        }
    }

    /**
     * 定时任务的处理逻辑 i
     */
    @Override
    public void run() {
        assert executor().inEventLoop();
        try {

            // 先进行时间校验
            if (delayNanos() > 0L) {
                // 得到的延迟时间 大于 0，说明还没到执行时间
                // Not yet expired, need to add or remove from queue
                if (isCancelled()) {
                    // 如果已经取消，则移除
                    scheduledExecutor().scheduledTaskQueue().removeTyped(this);
                } else {
                    // 否则重新添加到任务队列
                    scheduledExecutor().scheduleFromEventLoop(this);
                }
                return;
            }

            // 无重复
            if (periodNanos == 0) {
                if (setUncancellableInternal()) {
                    // 执行任务，得到结果
                    V result = runTask();
                    // 将结果设置回异步器
                    setSuccessInternal(result);
                }
            } else {

                // 其他情况
                // 先检查是否未取消
                // check if is done as it may was cancelled
                if (!isCancelled()) {
                    // 执行任务
                    runTask();
                    if (!executor().isShutdown()) {
                        // 判断当前线程池未关闭
                        if (periodNanos > 0) {
                            // 如果 periodNanos 大于 0，以固定速率执行
                            // 则下次执行时间 deadlineNanos 为之前的执行时间 + periodNanos
                            deadlineNanos += periodNanos;
                        } else {
                            // periodNanos < 0，以延迟的方式执行
                            // 则下次执行的时间 deadlineNanos 为当前的时间减去 periodNanos（注意，这里的 periodNanos 是负数）
                            deadlineNanos = scheduledExecutor().getCurrentTimeNanos() - periodNanos;
                        }
                        if (!isCancelled()) {
                            //  未取消，则将当前任务重新入队列
                            scheduledExecutor().scheduleFromEventLoop(this);
                        }
                    }
                }
            }
        } catch (Throwable cause) {
            setFailureInternal(cause);
        }
    }

    private AbstractScheduledEventExecutor scheduledExecutor() {
        return (AbstractScheduledEventExecutor) executor();
    }

    /**
     * {@inheritDoc}
     *
     * @param mayInterruptIfRunning this value has no effect in this implementation.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean canceled = super.cancel(mayInterruptIfRunning);
        if (canceled) {
            scheduledExecutor().removeScheduled(this);
        }
        return canceled;
    }

    boolean cancelWithoutRemove(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected StringBuilder toStringBuilder() {
        StringBuilder buf = super.toStringBuilder();
        buf.setCharAt(buf.length() - 1, ',');

        return buf.append(" deadline: ")
                  .append(deadlineNanos)
                  .append(", period: ")
                  .append(periodNanos)
                  .append(')');
    }

    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue) {
        return queueIndex;
    }

    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i) {
        queueIndex = i;
    }
}

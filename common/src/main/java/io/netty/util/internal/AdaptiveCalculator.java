/*
 * Copyright 2025 The Netty Project
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
package io.netty.util.internal;

import java.util.ArrayList;
import java.util.List;

import static io.netty.util.internal.ObjectUtil.checkPositive;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Calculate sizes in a adaptive way.
 */
public final class AdaptiveCalculator {
    private static final int INDEX_INCREMENT = 4;
    private static final int INDEX_DECREMENT = 1;

    private static final int[] SIZE_TABLE;

    static {
        List<Integer> sizeTable = new ArrayList<Integer>();
        // 16，16*2，16*3，16*4 ... 511
        // 也就是从 16 到 511，递增 16
        for (int i = 16; i < 512; i += 16) {
            sizeTable.add(i);
        }

        // 从 512 到 65535，512，1024，2048，以2倍的情况累加
        // Suppress a warning since i becomes negative when an integer overflow happens
        for (int i = 512; i > 0; i <<= 1) {
            sizeTable.add(i);
        }

        SIZE_TABLE = new int[sizeTable.size()];
        for (int i = 0; i < SIZE_TABLE.length; i ++) {
            SIZE_TABLE[i] = sizeTable.get(i);
        }
    }

    private static int getSizeTableIndex(final int size) {
        for (int low = 0, high = SIZE_TABLE.length - 1;;) {
            if (high < low) {
                return low;
            }
            if (high == low) {
                return high;
            }

            int mid = low + high >>> 1;
            int a = SIZE_TABLE[mid];
            int b = SIZE_TABLE[mid + 1];
            if (size > b) {
                low = mid + 1;
            } else if (size < a) {
                high = mid - 1;
            } else if (size == a) {
                return mid;
            } else {
                return mid + 1;
            }
        }
    }

    private final int minIndex;
    private final int maxIndex;
    private final int minCapacity;
    private final int maxCapacity;
    private int index;
    private int nextSize;
    private boolean decreaseNow;

    public AdaptiveCalculator(int minimum, int initial, int maximum) {
        checkPositive(minimum, "minimum");
        if (initial < minimum) {
            throw new IllegalArgumentException("initial: " + initial);
        }
        if (maximum < initial) {
            throw new IllegalArgumentException("maximum: " + maximum);
        }

        int minIndex = getSizeTableIndex(minimum);
        if (SIZE_TABLE[minIndex] < minimum) {
            this.minIndex = minIndex + 1;
        } else {
            this.minIndex = minIndex;
        }

        int maxIndex = getSizeTableIndex(maximum);
        if (SIZE_TABLE[maxIndex] > maximum) {
            this.maxIndex = maxIndex - 1;
        } else {
            this.maxIndex = maxIndex;
        }

        int initialIndex = getSizeTableIndex(initial);
        if (SIZE_TABLE[initialIndex] > initial) {
            this.index = initialIndex - 1;
        } else {
            this.index = initialIndex;
        }
        this.minCapacity = minimum;
        this.maxCapacity = maximum;
        nextSize = max(SIZE_TABLE[index], minCapacity);
    }

    /**
     * 接收数据的 buffer 的容量会尽可能的足够大以接收数据
     * 也尽可能的小以避免内存碎片
     * @param size
     */
    public void record(int size) {

        // 尝试是否可以减小分配的空间仍然能满足需求，
        // 尝试方法：当前读取的 size 是否小于或等于 打算缩小的的大小
        if (size <= SIZE_TABLE[max(0, index - INDEX_DECREMENT)]) {
            if (decreaseNow) {
                // decreaseNow 连续减小2次都可以
                index = max(index - INDEX_DECREMENT, minIndex);
                nextSize = max(SIZE_TABLE[index], minCapacity);
                decreaseNow = false;
            } else {
                decreaseNow = true;
            }
        } else if (size >= nextSize) {
            // 判断是否实际读取的数据大小等于预估的，是则尝试扩容

            index = min(index + INDEX_INCREMENT, maxIndex);
            nextSize = min(SIZE_TABLE[index], maxCapacity);
            decreaseNow = false;
        }
    }

    public int nextSize() {
        return nextSize;
    }
}

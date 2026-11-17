/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * ThreadFilter to exclude ThreadLeak checks for Arrow's {@code allocation.logger} daemon.
 *
 * <p>Arrow's netty-backed allocator ({@code PooledByteBufAllocatorL$InnerAllocator$MemoryStatusThread})
 * lazily starts a single, JVM-lifetime daemon thread named {@code allocation.logger} the first time the
 * allocator singleton is loaded while the {@code arrow.allocator} logger has TRACE enabled. It periodically
 * logs allocator memory statistics and exposes no stop API, so it cannot be shut down between test suites.
 * Because it is a low-priority daemon that neither holds test resources nor prevents JVM exit, leaking it is
 * benign; we simply exclude it from the suite-scope thread-leak check.
 */
public class ArrowAllocationLoggerThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return "allocation.logger".equals(t.getName());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The {@link org.testcontainers.images.TimeLimitedLoggedPullImageResultCallback} instance used by test containers,
 *  creates a watcher daemon thread which is never
 * stopped. This filter excludes that thread from the thread leak detection logic. It also excludes ryuk resource reaper
 * thread aws IdleConnectionReaper thread, which are not closed on time .
 */
public final class TestContainerThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("testcontainers-pull-watchdog-")
            || t.getName().startsWith("testcontainers-ryuk")
            || t.getName().startsWith("idle-connection-reaper");
    }
}

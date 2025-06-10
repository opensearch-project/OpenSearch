/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The {@link org.testcontainers.images.TimeLimitedLoggedPullImageResultCallback} instance used by test containers,
 * for example {@link org.testcontainers.containers.KafkaContainer} creates a watcher daemon thread which is never
 * stopped. This filter excludes that thread from the thread leak detection logic. It also excludes ryuk resource reaper
 * thread and pollers which is not closed on time.
 */
public final class TestContainerThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("testcontainers-pull-watchdog-")
            || t.getName().startsWith("testcontainers-ryuk")
            || t.getName().startsWith("stream-poller-consumer");
    }
}

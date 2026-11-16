/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The {@link org.testcontainers.images.TimeLimitedLoggedPullImageResultCallback} instance used by test containers,
 * creates a watcher daemon thread which is never stopped. This filter excludes that thread from the thread leak
 * detection logic. It also excludes the ryuk resource reaper thread, the aws IdleConnectionReaper thread, and the
 * native {@code AwsEventLoop} threads (created by the AWS CRT client used by {@code repository-s3}), which are not
 * closed on time.
 *
 * <p>{@code response-input-stream-timeout-scheduler} is excluded for the same reason
 * {@code repository-s3} excludes it in its own suites: the AWS SDK schedules {@code ResponseInputStream} timeouts on a
 * JVM-wide, statically initialized executor that it exposes no way to shut down, so closing the S3 client does not
 * reclaim the thread. It is a daemon that times out after 60 seconds of inactivity, so it neither blocks JVM shutdown
 * nor accumulates.
 */
public final class TestContainerThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("testcontainers-pull-watchdog-")
            || t.getName().startsWith("testcontainers-ryuk")
            || t.getName().startsWith("idle-connection-reaper")
            || t.getName().startsWith("AwsEventLoop")
            || t.getName().startsWith("response-input-stream-timeout-scheduler");
    }
}

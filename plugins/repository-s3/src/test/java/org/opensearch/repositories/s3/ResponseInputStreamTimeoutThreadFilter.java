/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The AWS SDK schedules {@code ResponseInputStream} timeouts on a JVM-wide, statically initialized
 * {@code ScheduledExecutorService} ({@code ResponseInputStream.TimeoutScheduler#INSTANCE}). The SDK exposes no way to
 * shut it down, so closing an S3 client does not reclaim its thread and the suite scope sees it as leaked. The thread is
 * a daemon and the pool lets it time out after 60 seconds of inactivity, so it neither blocks JVM shutdown nor
 * accumulates. Filter it out.
 */
public class ResponseInputStreamTimeoutThreadFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("response-input-stream-timeout-scheduler");
    }
}

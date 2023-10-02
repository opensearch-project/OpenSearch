/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.apache.lucene.store.RateLimiter;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The stream limiter that limits the transfer of bytes
 *
 * @opensearch.internal
 */
public class StreamLimiter {

    private final Supplier<RateLimiter> rateLimiterSupplier;

    private final StreamLimiter.Listener listener;

    private int bytesSinceLastRateLimit;

    public StreamLimiter(Supplier<RateLimiter> rateLimiterSupplier, Listener listener) {
        this.rateLimiterSupplier = rateLimiterSupplier;
        this.listener = listener;
    }

    public void maybePause(int bytes) throws IOException {
        bytesSinceLastRateLimit += bytes;
        final RateLimiter rateLimiter = rateLimiterSupplier.get();
        if (rateLimiter != null) {
            if (bytesSinceLastRateLimit >= rateLimiter.getMinPauseCheckBytes()) {
                long pause = rateLimiter.pause(bytesSinceLastRateLimit);
                bytesSinceLastRateLimit = 0;
                if (pause > 0) {
                    listener.onPause(pause);
                }
            }
        }
    }

    /**
     * Internal listener
     *
     * @opensearch.internal
     */
    public interface Listener {
        void onPause(long nanos);
    }
}

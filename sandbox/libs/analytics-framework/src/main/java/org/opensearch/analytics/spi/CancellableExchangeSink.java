/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Optional extension of {@link ExchangeSink} for sinks that support immediate
 * cancellation of in-flight native execution. When a coordinator-reduce stage
 * is cancelled or fails, the stage executor calls {@link #cancel()} before
 * {@link ExchangeSink#close()} so the sink can abort the running computation
 * immediately rather than waiting for it to drain to completion naturally.
 *
 * @opensearch.internal
 */
public interface CancellableExchangeSink extends ExchangeSink {

    /**
     * Signals immediate cancellation of any in-flight native execution.
     * Must be safe to call concurrently with {@link #feed} and {@link #close}.
     * Must be idempotent. After this returns, any blocking drain should
     * unblock promptly.
     */
    void cancel();
}

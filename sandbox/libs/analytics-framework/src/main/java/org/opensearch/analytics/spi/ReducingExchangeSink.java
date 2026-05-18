/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.action.ActionListener;

/**
 * {@link ExchangeSink} that runs reduce work (execute plan, drain into downstream).
 * The reduce-stage execution invokes {@link #reduce} from its task; {@link #close} runs
 * separately on every terminal transition for resource cleanup — split so cancel-before-reduce
 * paths can release resources without re-running the work.
 *
 * @opensearch.experimental
 */
public interface ReducingExchangeSink extends ExchangeSink {

    /**
     * Execute the reduce plan and drain output into the configured downstream; fire
     * {@code listener} on completion. Single-shot. Must not release resources — that's
     * {@link #close()}'s job.
     */
    void reduce(ActionListener<Void> listener);

    /**
     * {@code true} (default) — {@link #reduce} can run concurrently with producer
     * {@link #feed} calls. Buffered sinks that need the complete input set first
     * (e.g. memtable-backed) must override to {@code false}.
     */
    default boolean supportsEagerScheduling() {
        return true;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Coordinator-side {@link ExchangeSink} that exposes a per-child sub-sink for
 * each child stage feeding into it.
 *
 * <p>Used by multi-input shapes (currently {@code UNION}; future {@code JOIN}).
 * The orchestrator obtains a wrapper via {@link #sinkForChild(int)} for each
 * child stage so that each child feeds into its own input partition on the
 * backend's native session. The parent sink's lifecycle ({@link #close()}) is
 * still driven by the orchestrator and runs after every child wrapper's
 * {@link ExchangeSink#close()} has been called.
 *
 * @opensearch.internal
 */
public interface MultiInputExchangeSink extends ExchangeSink {

    /**
     * Returns the sink that the orchestrator should route the named child
     * stage's output into. Implementations bind each returned wrapper to a
     * distinct input partition (typically named {@code "input-<stageId>"})
     * registered on the backend's native session at sink construction.
     */
    ExchangeSink sinkForChild(int childStageId);
}

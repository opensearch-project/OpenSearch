/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Factory for creating a coordinator-side {@link ExchangeSink}.
 *
 * <p>A backend that can accept Arrow Record Batches from data nodes and run
 * coordinator-side computation over them (final aggregate, sort, etc.) implements
 * this interface.
 *
 * <p>Returned by {@link AnalyticsSearchBackendPlugin#getExchangeSinkProvider()}.
 * A {@code null} return means the backend cannot act as a coordinator-side executor.
 *
 * @opensearch.internal
 */
public interface ExchangeSinkProvider {

    /**
     * Creates a sink for coordinator-side execution. The backend implementation
     * uses {@link ExchangeSinkContext#fragmentBytes()} as the serialized plan
     * (produced by {@link FragmentConvertor#convertFinalAggFragment}) and
     * writes its reduced output into {@link ExchangeSinkContext#downstream()}.
     *
     * @param context core-provided context carrying plan bytes, allocator, child inputs, and downstream sink
     * @param backendContext backend-opaque state produced by instruction handlers (e.g.
     *        {@code FinalAggregateInstructionHandler}), or {@code null} when no handler ran
     */
    ExchangeSink createSink(ExchangeSinkContext context, BackendExecutionContext backendContext);

    /**
     * Returns the Arrow schema the data-node prepared physical plan will emit
     * for the given partial-aggregate plan bytes, or {@code null} if the backend
     * has no opinion (caller falls back to a row-type-based derivation).
     *
     * @param partialAggBytes bytes from a prior {@link FragmentConvertor#attachPartialAggOnTop}
     */
    default Schema partialAggOutputSchema(byte[] partialAggBytes) {
        return null;
    }
}

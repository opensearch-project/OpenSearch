/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

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
     * (produced by {@code FragmentConvertor#convertFragment}) and writes its
     * reduced output into {@link ExchangeSinkContext#downstream()}.
     *
     * <p>The schema of each child's batches is learned at the backend boundary
     * (not pre-declared on {@link ExchangeSinkContext.ChildInput}) — the backend
     * derives it when it registers the child input on its native session, since
     * the producer-side plan bytes already encode the producer schema.
     *
     * @param context core-provided context carrying plan bytes, allocator, child inputs, and downstream sink
     * @param backendContext backend-opaque state produced by instruction handlers (e.g.
     *        {@code FinalAggregateInstructionHandler}), or {@code null} when no handler ran
     */
    ExchangeSink createSink(ExchangeSinkContext context, BackendExecutionContext backendContext);
}

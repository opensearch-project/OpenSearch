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
     * (produced by {@link FragmentConvertor#convertFinalAggFragment}) and
     * writes its reduced output into {@link ExchangeSinkContext#downstream()}.
     */
    ExchangeSink createSink(ExchangeSinkContext context);
}

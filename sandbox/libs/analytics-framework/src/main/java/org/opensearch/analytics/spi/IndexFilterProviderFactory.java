/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Builds an {@link IndexFilterProvider} from the serialized query bytes that
 * appear in the substrait plan's {@code index_filter(bytes)} call.
 *
 * <p>Exactly one factory is registered per JVM, typically by the analytics
 * plugin that owns the backend (e.g. inverted index, sparse vector, etc.). The native engine calls
 * it once per Collector leaf per query; the returned provider stays alive
 * for the query's duration.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFilterProviderFactory {

    /**
     * Build a provider from opaque query bytes. Implementations typically
     * deserialize the bytes into a backend-native query, compile it against
     * the current catalog snapshot, and wrap the compiled form as an
     * {@link IndexFilterProvider}.
     *
     * @throws Exception on any failure (wrapped and routed to Rust as {@code -1}).
     */
    IndexFilterProvider create(byte[] queryBytes) throws Exception;
}

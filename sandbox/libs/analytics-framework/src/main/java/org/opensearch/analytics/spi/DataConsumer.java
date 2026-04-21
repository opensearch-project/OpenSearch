/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Implemented by types whose children write row batches into them, keyed by
 * child stage id. Used by the framework during construction to resolve the
 * {@link ExchangeSink} a child stage should write into, and by backend-provided
 * compute units that accept per-child input fan-in.
 *
 * @opensearch.internal
 */
public interface DataConsumer {

    /**
     * Returns the {@link ExchangeSink} that the given child stage
     * should write its output into.
     */
    ExchangeSink inputSink(int childStageId);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSink;

/**
 * Implemented by {@link StageExecution} types whose children write
 * row batches into them. Used by the builder during construction to
 * resolve the {@link ExchangeSink} a child stage should write into.
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

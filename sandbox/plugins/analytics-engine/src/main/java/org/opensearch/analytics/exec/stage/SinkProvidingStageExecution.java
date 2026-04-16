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
 * Implemented by {@link StageExecution} types whose children may write
 * row batches into them. Used by the walker during construction to thread
 * a sink from parent to row-producing child without an instanceof cascade
 * over concrete execution types.
 *
 * @opensearch.internal
 */
public interface SinkProvidingStageExecution extends StageExecution {
    /**
     * Returns the {@link ExchangeSink} that the given child stage should write
     * into.
     */
    default ExchangeSink sink(int childStageId) {
        return sink();
    }

    ExchangeSink sink();
}

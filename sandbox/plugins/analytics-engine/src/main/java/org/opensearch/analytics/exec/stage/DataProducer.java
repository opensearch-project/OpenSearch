/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.spi.DataConsumer;

/**
 * Implemented by {@link StageExecution} types whose accumulated results can
 * be read by a downstream consumer (parent stage, or the walker's completion
 * listener for the root stage).
 *
 * @see DataConsumer for the complementary input-side contract
 * @opensearch.internal
 */
public interface DataProducer {

    /**
     * Returns the read-side source for consuming this stage's output.
     */
    ExchangeSource outputSource();
}

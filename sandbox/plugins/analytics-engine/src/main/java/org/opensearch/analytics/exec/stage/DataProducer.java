/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.ExchangeSource;

/**
 * Implemented by {@link StageExecution} types that write row batches
 * into an output sink. The output sink is typically owned by the
 * parent stage (resolved via {@link DataConsumer#inputSink}).
 *
 * <p>Exposes both the write-side ({@link ExchangeSink}) for producers
 * feeding batches and the read-side ({@link ExchangeSource}) for the
 * walker to consume final results.
 *
 * @opensearch.internal
 */
public interface DataProducer {

    /**
     * Returns the write-side sink this stage feeds batches into.
     */
    ExchangeSink outputSink();

    /**
     * Returns the read-side source for consuming accumulated results.
     * For stages backed by a {@code RowProducingSink}, this returns the
     * same object as {@link #outputSink()} (which implements both interfaces).
     */
    ExchangeSource outputSource();
}

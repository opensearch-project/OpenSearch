/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.spi.DataConsumer;

/**
 * Combines {@link DataConsumer} and {@link DataProducer} for stages that
 * both accept child input and produce output (root gather, local compute).
 *
 * <p>Stages that own a single shared sink (like {@link PassThroughStageExecution})
 * return the same object for both {@link DataConsumer#inputSink(int)} and
 * {@link DataProducer#outputSource()}. Stages with backend-provided per-child
 * routing (like {@link LocalStageExecution}) delegate both methods to the
 * backend's fragment executor.
 *
 * @opensearch.internal
 */
public interface SinkProvidingStageExecution extends StageExecution, DataConsumer, DataProducer {
}

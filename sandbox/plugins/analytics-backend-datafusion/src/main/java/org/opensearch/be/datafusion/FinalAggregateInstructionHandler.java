/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;

/**
 * Handles FinalAggregate instruction for coordinator-reduce stages.
 * TODO: Configure SessionContext optimizer (disable CombinePartialFinalAggregate) for multi-shard aggregates.
 */
public class FinalAggregateInstructionHandler implements FragmentInstructionHandler<FinalAggregateInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        FinalAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        // TODO: Configure LocalSession optimizer settings for final aggregate execution.
        // For now, the reduce path works without explicit configuration.
        return backendContext;
    }
}

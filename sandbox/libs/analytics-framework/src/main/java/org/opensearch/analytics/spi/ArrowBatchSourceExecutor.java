/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.tasks.Task;

/**
 * Executes an engine plan over a caller-provided Arrow pull source.
 *
 * <p>This SPI keeps storage plugins independent of the execution-engine implementation.
 * The executor takes ownership of {@code sourceFactory} when {@link #execute} is called,
 * including when execution setup fails. The returned stream owns all remaining native and
 * callback resources and releases them from {@link EngineResultStream#close()}.
 *
 * @opensearch.internal
 */
public interface ArrowBatchSourceExecutor {

    /**
     * Compiles a fragment rebased onto the named Arrow input in its
     * {@link ArrowBatchSourcePlan}. Partial aggregates must emit intermediate state for the
     * coordinator merge rather than finalized values.
     */
    default byte[] compile(RelNode fragment, boolean partialAggregate) {
        throw new UnsupportedOperationException("Arrow batch source plan compilation is not supported");
    }

    /** Attaches one output operator to an already compiled Arrow source plan. */
    default byte[] attachFragment(RelNode fragment, byte[] innerPlanBytes) {
        throw new UnsupportedOperationException("Arrow batch source plan composition is not supported");
    }

    EngineResultStream execute(
        BufferAllocator resultAllocator,
        ArrowBatchSourcePlan plan,
        ArrowBatchSourceFactory sourceFactory,
        Task task,
        DelegationThreadTracker threadTracker
    );
}

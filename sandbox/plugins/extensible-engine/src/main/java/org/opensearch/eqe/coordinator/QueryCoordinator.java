/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.coordinator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.opensearch.tasks.Task;

import java.util.concurrent.CompletableFuture;

/**
 * Coordinator responsible for orchestrating query execution across the cluster.
 *
 *
 * <p>Accepts a logical plan ({@link RelNode}) and the parent {@link Task} and returns a single {@link VectorSchemaRoot}
 */
public interface QueryCoordinator {

    /**
     * Execute a logical plan by fanning out to data nodes and merging results.
     *
     * @param logicalPlan the Calcite logical plan to execute
     * @param parentTask  the parent task for child request tracking and cancellation
     * @return future containing merged results as Arrow VectorSchemaRoot
     */
    CompletableFuture<VectorSchemaRoot> execute(RelNode logicalPlan, Task parentTask);
}

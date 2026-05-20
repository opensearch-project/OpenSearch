/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import java.io.IOException;

/**
 * Plans the execution strategy for engine operations based on the shard's role.
 * This interface defines how operations (indexing, deletion) should be processed
 * differently depending on whether the shard is acting as a primary or replica.
 *
 * @opensearch.internal
 */
public interface OperationStrategyPlanner<T extends Engine.Operation, V extends OperationStrategy> {

    /**
     * Plans the execution strategy for an operation on a primary shard.
     *
     * @param operation the operation to plan (index or delete)
     * @return the strategy for executing this operation on the primary
     * @throws IOException if an I/O error occurs during planning
     */
    V planOperationAsPrimary(T operation) throws IOException;

    /**
     * Plans the execution strategy for an operation on a non-primary (replica) shard.
     *
     * @param operation the operation to plan (index or delete)
     * @return the strategy for executing this operation on the replica
     * @throws IOException if an I/O error occurs during planning
     */
    V planOperationAsNonPrimary(T operation) throws IOException;

    default boolean assertNonPrimaryOrigin(final T operation) {
        assert operation.origin() != Engine.Operation.Origin.PRIMARY : "planing as primary but got " + operation.origin();
        return true;
    }
}

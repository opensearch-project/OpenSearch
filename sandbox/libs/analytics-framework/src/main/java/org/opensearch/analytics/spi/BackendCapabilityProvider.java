/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares the query planning capabilities of a backend plugin.
 * Used by the coordinator-side planner to determine which backends can
 * evaluate each operator, predicate, aggregate call, and projection expression.
 *
 * <p>All methods except {@link #supportedOperators()} throw {@link UnsupportedOperationException}
 * by default so that backends fail fast and explicitly rather than silently returning empty sets.
 * {@link #supportedOperators()} is abstract — a backend with no operator support has no purpose.
 *
 * @opensearch.internal
 */
public interface BackendCapabilityProvider {

    /** Relational operators this backend can execute as the primary operator. */
    Set<OperatorCapability> supportedOperators();

    /** Filter predicates this backend can evaluate, scoped to operator, field type, and data format. */
    default Set<FilterCapability> filterCapabilities() {
        return Set.of();
    }

    /** Aggregate functions this backend can evaluate, scoped to function, field type, and data format. */
    default Set<AggregateCapability> aggregateCapabilities() {
        return Set.of();
    }

    /** Projection expressions this backend can evaluate, scoped to function/name and data format. */
    default Set<ProjectCapability> projectCapabilities() {
        return Set.of();
    }

    /** Window functions this backend can evaluate, scoped to function, field type, and data format. */
    default Set<WindowCapability> windowCapabilities() {
        return Set.of();
    }

    /**
     * Delegation types this backend can initiate — it has a custom physical operator
     * that calls Analytics Core's delegation API to offload work to another backend.
     */
    default Set<DelegationType> supportedDelegations() {
        return Set.of();
    }

    /**
     * Delegation types this backend can accept — it can receive a delegated request
     * (e.g., a serialized QueryBuilder) and return results (e.g., a bitset of matching docIds).
     */
    default Set<DelegationType> acceptedDelegations() {
        return Set.of();
    }

    /**
     * Operators this backend can execute on Arrow batches, regardless of source
     * (shard scan by another backend, shuffle read, in-memory). If an operator is NOT
     * in this set, the backend requires its own native data structures (inverted index,
     * BKD tree, etc.) and cannot operate on externally produced Arrow batches.
     */
    default Set<OperatorCapability> arrowCompatibleOperators() {
        return Set.of();
    }

    /** Shuffle write modes this backend supports at data nodes. */
    default Set<ShuffleCapability> supportedShuffleCapabilities() {
        return Set.of();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Map;
import java.util.Set;

/**
 * Declares the query planning capabilities of a backend plugin.
 * Used by the coordinator-side planner to determine which backends can
 * evaluate each operator, predicate, aggregate call, and projection expression.
 *
 * <p>A capability type belongs here only if a planner rule uses it to make a
 * decision. Backend-internal optimizations (e.g. expression pushdown into scan)
 * are not declared here.
 *
 * @opensearch.internal
 */
public interface BackendCapabilityProvider {

    /** Relational operators this backend can execute as the primary operator. */
    Set<EngineCapability> supportedEngineCapabilities();

    /** Storage sources this backend can scan, scoped to storage kind, formats, and field types. */
    default Set<ScanCapability> scanCapabilities() {
        return Set.of();
    }

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
     * Per-function adapters for transforming backend-agnostic scalar function RexCalls
     * into backend-compatible forms before fragment conversion. Keyed by {@link ScalarFunction}.
     * Applied regardless of operator context (filter, project, aggregate expression).
     * Empty map means no adaptation needed.
     */
    default Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
        return Map.of();
    }
}

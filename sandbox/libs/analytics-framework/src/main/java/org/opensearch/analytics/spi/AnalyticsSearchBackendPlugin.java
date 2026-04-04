/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Collections;
import java.util.Set;

/**
 * SPI extension point for back-end query engines for query planning and execution capabilities
 * as needed by the {@link org.opensearch.analytics.exec.QueryPlanExecutor}.
 * <p>
 * Storage format declarations ({@code getSupportedFormats()}) belong on
 * {@link org.opensearch.plugins.SearchBackEndPlugin} — the planner accesses
 * field storage via {@code FieldStorageResolver} which reads from the storage layer.
 * as needed by the {@link org.opensearch.analytics.exec.QueryPlanExecutor}
 *
 * <p>TODO: separate capability declaration (planner, coordinator) from execution engine factory
 * (data node) into two interfaces. AnalyticsSearchBackendPlugin should only declare capabilities.
 * SearchExecEngineProvider should be discovered separately by the executor. Remove the extends
 * relationship and the default createSearchExecEngine() below once that separation is done.
 *
 * <p>TODO: move capability methods (filterCapabilities, aggregateCapabilities, windowCapabilities,
 * projectCapabilities, supportedOperators, supportedDelegations, acceptedDelegations,
 * supportedShuffleCapabilities) into a dedicated CapabilityAPI interface.
 */
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider {

    /** Filter capabilities scoped to operator, field type, and data format. */
    default Set<FilterCapability> filterCapabilities() {
        return Collections.emptySet();
    }

    /** Relational operations this backend can perform. */
    default Set<OperatorCapability> supportedOperators() {
        return Collections.emptySet();
    }

    /** Delegation types this backend can initiate (has custom physical operators to call delegation API). */
    default Set<DelegationType> supportedDelegations() {
        return Collections.emptySet();
    }

    /** Delegation types this backend can accept from others (can serve delegated requests). */
    default Set<DelegationType> acceptedDelegations() {
        return Collections.emptySet();
    }

    /** Aggregate capabilities scoped to function, field type, and data format. */
    default Set<AggregateCapability> aggregateCapabilities() {
        return Collections.emptySet();
    }

    /** Window capabilities scoped to function, field type, and data format. */
    default Set<WindowCapability> windowCapabilities() {
        return Collections.emptySet();
    }

    /** Project capabilities: scalar functions and opaque operations, scoped to data format. */
    default Set<ProjectCapability> projectCapabilities() {
        return Collections.emptySet();
    }

    /** Shuffle write modes this backend supports at data nodes. */
    default Set<ShuffleCapability> supportedShuffleCapabilities() {
        return Collections.emptySet();
    }

    /**
     * Operators this backend can execute on Arrow batches from another backend's scan.
     * If an operator is NOT in this set, the backend can only execute it on data from
     * its own scan (uses internal data structures like inverted index, BKD tree, etc.).
     *
     * <p>Example: DataFusion declares {FILTER, AGGREGATE, SORT, PROJECT} — it operates
     * on Arrow column vectors regardless of source. Lucene declares {} — it needs its
     * own segment-level data structures for all operators.
     */
    default Set<OperatorCapability> arrowCompatibleOperators() {
        return Collections.emptySet();
    }

    /**
     * Returns the fragment converter for this backend.
     * Analytics-engine calls methods on this to convert resolved fragments
     * into the backend's native serializable form.
     */
    default FragmentConvertor getFragmentConvertor() {
        throw new UnsupportedOperationException("getFragmentConvertor not implemented for " + name());
    }

    /**
     * Converts a RelNode fragment into a backend-serializable form.
     * E.g. Substrait for DataFusion, QueryBuilder bytes for Lucene.
     * TODO: remove once all callers migrate to getFragmentConvertor()
     */
    default byte[] convertFragment(Object fragment) {
        throw new UnsupportedOperationException("convertFragment not yet implemented for " + name());
    }
}

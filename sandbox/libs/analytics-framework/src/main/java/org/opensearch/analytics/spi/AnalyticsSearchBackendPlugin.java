/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * SPI extension point for back-end query engines for query planning and execution capabilities
 * as needed by the {@link org.opensearch.analytics.exec.QueryPlanExecutor}
 *
 * <p>TODO: separate capability declaration (planner, coordinator) from execution engine factory
 * (data node) into two interfaces. AnalyticsSearchBackendPlugin should only declare capabilities.
 * SearchExecEngineProvider should be discovered separately by the executor. Remove the extends
 * relationship and the default createSearchExecEngine() below once that separation is done.
 */
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider {

    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /**
     * {@inheritDoc}
     * Temporary default — remove once SearchExecEngineProvider is separated from this interface.
     */
    @Override
    default SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        throw new UnsupportedOperationException("createSearchExecEngine not implemented for " + name());
    }

    /** Returns the data formats supported by this backend. */
    List<DataFormat> getSupportedFormats();

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
     * Converts a RelNode fragment into a backend-serializable form.
     * E.g. Substrait for DataFusion, QueryBuilder bytes for Lucene.
     * TODO: implement in Phase 2 (fragment conversion).
     */
    default byte[] convertFragment(Object fragment) {
        throw new UnsupportedOperationException("convertFragment not yet implemented for " + name());
    }
}

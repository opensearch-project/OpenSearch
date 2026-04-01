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
 */
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider {

    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /**
     * Creates a searcher bound to the given reader snapshot.
     * @param ctx the execution context
     */
    SearchExecEngine<ExecutionContext, EngineResultStream> searcher(ExecutionContext ctx);

    /** Returns the data formats supported by this backend. */
    List<DataFormat> getSupportedFormats();

    /** Filter operators this backend can evaluate, scoped by field type family. */
    default Set<FilterCapability> supportedFilterCapabilities() {
        return Collections.emptySet();
    }

    /** Full-text operators this backend can evaluate on indexed fields. */
    default Set<FullTextOperator> supportedFullTextOperators() {
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

    /** Aggregate functions this backend can evaluate (SUM, AVG, COUNT, etc.). */
    default Set<AggregateFunction> supportedAggregateFunctions() {
        return Collections.emptySet();
    }

    /** Scalar functions this backend can evaluate in projections (UPPER, CAST, math ops, etc.). */
    default Set<ScalarFunction> supportedScalarFunctions() {
        return Collections.emptySet();
    }

    /**
     * Names of opaque project operations this backend can handle (e.g. "painless", "highlight", "suggest").
     * Used to resolve UnresolvedRexNode from frontend plugins to a backend.
     * Analytics Core is agnostic to what these names mean — backends define and consume them.
     */
    default Set<String> supportedOpaqueProjectOperations() {
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

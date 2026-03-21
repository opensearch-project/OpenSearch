/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.util.Set;


/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.)
 * @opensearch.internal
 */
public interface AnalyticsBackEndPlugin {
    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /** JNI boundary for executing serialized plans, or null for engines without native execution. */
    EngineBridge<?, ?, ?> bridge(CompositeEngine engine, CatalogSnapshot snapshot);

    /** Supported functions as a Calcite operator table, or null if the back-end adds no functions. */
    SqlOperatorTable operatorTable();

    /** Whether this backend supports the SearchExecEngine path via CompositeEngine. */
    default boolean supportsSearchExecEngine() {
        return false;
    }

    /** Returns the set of RelNode operator classes this backend supports. */
    default Set<Class<? extends RelNode>> supportedOperators() {
        return Set.of(
            org.apache.calcite.rel.logical.LogicalTableScan.class,
            org.apache.calcite.rel.logical.LogicalFilter.class,
            org.apache.calcite.rel.logical.LogicalAggregate.class,
            org.apache.calcite.rel.logical.LogicalProject.class
        );
    }

    /** Returns true if this backend can accept and execute the given opaque predicate payload. */
    default boolean canAcceptUnresolvedPredicate(byte[] payload) {
        return false;
    }
}

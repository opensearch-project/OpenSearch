/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Objects;

/**
 * Carries the shared state needed by converters and query translators
 * during DSL-to-RelNode conversion.
 */
public class ConversionContext {

    private final SearchSourceBuilder searchSource;
    private final RelOptCluster cluster;
    private final RelOptTable table;
    private final AggregationMetadata aggregationMetadata;

    /**
     * Creates a conversion context.
     *
     * @param searchSource the original DSL query
     * @param cluster the Calcite cluster for building expressions and RelNodes
     * @param table the resolved Calcite table for the target index
     */
    public ConversionContext(SearchSourceBuilder searchSource, RelOptCluster cluster, RelOptTable table) {
        this(searchSource, cluster, table, null);
    }

    private ConversionContext(
        SearchSourceBuilder searchSource,
        RelOptCluster cluster,
        RelOptTable table,
        AggregationMetadata aggregationMetadata
    ) {
        this.searchSource = Objects.requireNonNull(searchSource, "searchSource must not be null");
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.aggregationMetadata = aggregationMetadata;
    }

    /** Returns the original DSL query. */
    public SearchSourceBuilder getSearchSource() {
        return searchSource;
    }

    /** Returns the Calcite cluster. */
    public RelOptCluster getCluster() {
        return cluster;
    }

    /** Returns the resolved Calcite table. */
    public RelOptTable getTable() {
        return table;
    }

    /** Returns the index row type (field names and types). */
    public RelDataType getRowType() {
        return table.getRowType();
    }

    /** Returns the RexBuilder for creating expressions. */
    public RexBuilder getRexBuilder() {
        return cluster.getRexBuilder();
    }

    /** Returns the current aggregation metadata, or null if not in aggregation context. */
    public AggregationMetadata getAggregationMetadata() {
        return aggregationMetadata;
    }

    /**
     * Returns a new context with the given aggregation metadata.
     *
     * @param metadata the aggregation metadata to attach
     */
    public ConversionContext withAggregationMetadata(AggregationMetadata metadata) {
        return new ConversionContext(searchSource, cluster, table, metadata);
    }
}

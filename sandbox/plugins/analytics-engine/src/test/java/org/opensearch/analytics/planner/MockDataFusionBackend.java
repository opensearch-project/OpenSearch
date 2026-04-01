/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldTypeFamily;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.FullTextOperator;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mock DataFusion backend for tests. Supports parquet format, standard filter
 * operators on NUMERIC/KEYWORD/DATE/BOOLEAN families, and common aggregates.
 * No full-text support.
 */
public class MockDataFusionBackend implements AnalyticsSearchBackendPlugin {

    public static final String NAME = "mock-parquet";

    private static final Set<OperatorCapability> OPERATOR_CAPS = Set.of(
        OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.FILTER_ON_EXPRESSIONS,
        OperatorCapability.AGGREGATE, OperatorCapability.SORT,
        OperatorCapability.PROJECT, OperatorCapability.COORDINATOR_REDUCE
    );

    private static final Set<FilterOperator> FILTER_OPS = EnumSet.of(
        FilterOperator.EQUALS, FilterOperator.NOT_EQUALS,
        FilterOperator.GREATER_THAN, FilterOperator.GREATER_THAN_OR_EQUAL,
        FilterOperator.LESS_THAN, FilterOperator.LESS_THAN_OR_EQUAL,
        FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL,
        FilterOperator.IN, FilterOperator.LIKE
    );

    private static final Set<FieldTypeFamily> SUPPORTED_FAMILIES = EnumSet.of(
        FieldTypeFamily.NUMERIC, FieldTypeFamily.KEYWORD,
        FieldTypeFamily.DATE, FieldTypeFamily.BOOLEAN
    );

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        FILTER_CAPS = FILTER_OPS.stream()
            .flatMap(op -> SUPPORTED_FAMILIES.stream().map(family -> FilterCapability.of(op, family)))
            .collect(Collectors.toUnmodifiableSet());
    }

    private static final Set<AggregateFunction> AGG_FUNCTIONS = EnumSet.of(
        AggregateFunction.SUM, AggregateFunction.SUM0,
        AggregateFunction.MIN, AggregateFunction.MAX,
        AggregateFunction.COUNT, AggregateFunction.AVG
    );

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> searcher(ExecutionContext ctx) {
        return null;
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(new DataFormat() {
            @Override public String name() { return "parquet"; }
            @Override public long priority() { return 0; }
            @Override public Set<FieldTypeCapabilities> supportedFields() { return Set.of(); }
        });
    }

    @Override
    public Set<FilterCapability> supportedFilterCapabilities() {
        return FILTER_CAPS;
    }

    @Override
    public Set<FullTextOperator> supportedFullTextOperators() {
        return Set.of();
    }

    @Override
    public Set<OperatorCapability> supportedOperators() {
        return OPERATOR_CAPS;
    }

    @Override
    public Set<AggregateFunction> supportedAggregateFunctions() {
        return AGG_FUNCTIONS;
    }
}

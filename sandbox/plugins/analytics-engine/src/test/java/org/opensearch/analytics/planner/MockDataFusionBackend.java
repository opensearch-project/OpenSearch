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
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchBackEndPlugin;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mock DataFusion backend for tests. Supports parquet format with columnar storage,
 * standard filter operators on NUMERIC/KEYWORD/DATE/BOOLEAN, and common aggregates.
 * No full-text support.
 */
public class MockDataFusionBackend implements AnalyticsSearchBackendPlugin, SearchBackEndPlugin<Object> {

    public static final String NAME = "mock-parquet";
    public static final String PARQUET_DATA_FORMAT = "parquet";
    private static final Set<String> DATAFUSION_FORMATS = Set.of(PARQUET_DATA_FORMAT);

    private static final Set<OperatorCapability> OPERATOR_CAPS = Set.of(
        OperatorCapability.SCAN, OperatorCapability.FILTER,
        OperatorCapability.AGGREGATE, OperatorCapability.SORT,
        OperatorCapability.PROJECT, OperatorCapability.COORDINATOR_REDUCE
    );

    private static final Set<FieldType> SUPPORTED_TYPES = new HashSet<>();
    static {
        SUPPORTED_TYPES.addAll(FieldType.numeric());
        SUPPORTED_TYPES.addAll(FieldType.keyword());
        SUPPORTED_TYPES.addAll(FieldType.date());
        SUPPORTED_TYPES.add(FieldType.BOOLEAN);
    }

    private static final Set<FilterOperator> STANDARD_OPS = Set.of(
        FilterOperator.EQUALS, FilterOperator.NOT_EQUALS,
        FilterOperator.GREATER_THAN, FilterOperator.GREATER_THAN_OR_EQUAL,
        FilterOperator.LESS_THAN, FilterOperator.LESS_THAN_OR_EQUAL,
        FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL,
        FilterOperator.IN, FilterOperator.LIKE
    );

    private static final Set<AggregateFunction> AGG_FUNCTIONS = Set.of(
        AggregateFunction.SUM, AggregateFunction.SUM0,
        AggregateFunction.MIN, AggregateFunction.MAX,
        AggregateFunction.COUNT, AggregateFunction.AVG
    );

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (FilterOperator op : STANDARD_OPS) {
            for (FieldType type : SUPPORTED_TYPES) {
                caps.add(new FilterCapability.Standard(op, type, DATAFUSION_FORMATS));
            }
        }
        FILTER_CAPS = caps;
    }

    private static final Set<AggregateCapability> AGG_CAPS;
    static {
        Set<AggregateCapability> caps = new HashSet<>();
        for (AggregateFunction func : AGG_FUNCTIONS) {
            for (FieldType type : SUPPORTED_TYPES) {
                caps.add(AggregateCapability.simple(func, type, DATAFUSION_FORMATS));
            }
        }
        AGG_CAPS = caps;
    }

    @Override public String name() { return NAME; }

    @Override public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) { return null; }

    private static final Set<OperatorCapability> ARROW_COMPATIBLE_OPS = Set.of(
        OperatorCapability.FILTER, OperatorCapability.AGGREGATE,
        OperatorCapability.SORT, OperatorCapability.PROJECT
    );

    @Override public Set<OperatorCapability> supportedOperators() { return OPERATOR_CAPS; }
    @Override public Set<FilterCapability> filterCapabilities() { return FILTER_CAPS; }
    @Override public Set<AggregateCapability> aggregateCapabilities() { return AGG_CAPS; }
    @Override public Set<OperatorCapability> arrowCompatibleOperators() { return ARROW_COMPATIBLE_OPS; }

    // ---- SearchBackEndPlugin (storage) ----

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(new DataFormat() {
            @Override public String name() { return PARQUET_DATA_FORMAT; }
            @Override public long priority() { return 0; }
            @Override public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of(
                    new FieldTypeCapabilities("integer", Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("long", Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("keyword", Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("text", Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("boolean", Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("date", Set.of(COLUMNAR_STORAGE))
                );
            }
        });
    }

    @Override
    public EngineReaderManager<Object> createReaderManager(DataFormat format, ShardPath shardPath) {
        return null;
    }
}

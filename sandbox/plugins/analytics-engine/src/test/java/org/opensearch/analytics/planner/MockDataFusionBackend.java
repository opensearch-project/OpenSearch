/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.JoinCapability;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.WindowCapability;
import org.opensearch.analytics.spi.WindowFunction;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mock DataFusion backend for tests. Supports parquet format with columnar storage,
 * standard filter operators on NUMERIC/KEYWORD/DATE/BOOLEAN, and common aggregates.
 * No full-text support.
 *
 * <p>Tests override only the capability methods they need — everything else
 * falls through to the defaults declared here.
 */
public class MockDataFusionBackend extends MockBackend implements SearchBackEndPlugin<Object> {

    public static final String NAME = "mock-parquet";
    public static final String PARQUET_DATA_FORMAT = "parquet";
    private static final Set<String> DATAFUSION_FORMATS = Set.of(PARQUET_DATA_FORMAT);

    private static final Set<EngineCapability> OPERATOR_CAPS = Set.of(EngineCapability.SORT, EngineCapability.VALUES);

    private static final Set<FieldType> SUPPORTED_TYPES = new HashSet<>();
    static {
        SUPPORTED_TYPES.addAll(FieldType.numeric());
        SUPPORTED_TYPES.addAll(FieldType.keyword());
        SUPPORTED_TYPES.addAll(FieldType.date());
        SUPPORTED_TYPES.add(FieldType.BOOLEAN);
    }

    private static final Set<ScalarFunction> STANDARD_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.IN,
        ScalarFunction.LIKE
    );

    private static final Set<AggregateFunction> AGG_FUNCTIONS = Set.of(
        AggregateFunction.SUM,
        AggregateFunction.SUM0,
        AggregateFunction.MIN,
        AggregateFunction.MAX,
        AggregateFunction.COUNT,
        AggregateFunction.AVG
    );

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (ScalarFunction op : STANDARD_OPS) {
            caps.add(new FilterCapability.Standard(op, SUPPORTED_TYPES, DATAFUSION_FORMATS));
        }
        FILTER_CAPS = caps;
    }

    private static final Set<AggregateCapability> AGG_CAPS;
    static {
        Set<AggregateCapability> caps = new HashSet<>();
        for (AggregateFunction func : AGG_FUNCTIONS) {
            caps.add(AggregateCapability.simple(func, SUPPORTED_TYPES, DATAFUSION_FORMATS));
        }
        AGG_CAPS = caps;
    }

    private static final Set<ScanCapability> SCAN_CAPS = Set.of(new ScanCapability.DocValues(DATAFUSION_FORMATS, SUPPORTED_TYPES));

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ExchangeSinkProvider getExchangeSinkProvider() {
        // Stub — real implementation provided by DataFusion backend
        return (context, backendContext) -> new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {}

            @Override
            public void close() {}
        };
    }

    @Override
    protected Set<EngineCapability> supportedEngineCapabilities() {
        return OPERATOR_CAPS;
    }

    @Override
    protected Set<JoinCapability> joinCapabilities() {
        return Set.of(
            new JoinCapability(
                Set.of(
                    JoinCapability.JoinKind.INNER,
                    JoinCapability.JoinKind.LEFT,
                    JoinCapability.JoinKind.RIGHT,
                    JoinCapability.JoinKind.FULL,
                    JoinCapability.JoinKind.SEMI,
                    JoinCapability.JoinKind.ANTI,
                    JoinCapability.JoinKind.CROSS
                ),
                Set.of(PARQUET_DATA_FORMAT)
            )
        );
    }

    @Override
    protected Set<WindowCapability> windowCapabilities() {
        return Set.of(
            new WindowCapability(
                Set.of(WindowFunction.SUM, WindowFunction.AVG, WindowFunction.COUNT, WindowFunction.MIN, WindowFunction.MAX),
                Set.of(PARQUET_DATA_FORMAT)
            )
        );
    }

    @Override
    protected Set<ScanCapability> scanCapabilities() {
        return SCAN_CAPS;
    }

    private static final Set<ScalarFunction> PROJECT_OPS = Set.of(
        // Arithmetic / math (emitted by Calcite's AggregateReduceFunctionsRule)
        ScalarFunction.PLUS,
        ScalarFunction.MINUS,
        ScalarFunction.TIMES,
        ScalarFunction.DIVIDE,
        ScalarFunction.POWER,
        // Type coercion / null handling
        ScalarFunction.CAST,
        ScalarFunction.COALESCE,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        // Conditional / comparisons (STDDEV_SAMP / VAR_SAMP emit CASE WHEN count > 1 …)
        ScalarFunction.CASE,
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        // Logical connectives (projection-side composition: `case(a and b, …)`)
        ScalarFunction.AND,
        ScalarFunction.OR,
        ScalarFunction.NOT
    );

    private static final Set<ProjectCapability> PROJECT_CAPS;
    static {
        Set<ProjectCapability> caps = new HashSet<>();
        for (ScalarFunction op : PROJECT_OPS) {
            caps.add(new ProjectCapability.Scalar(op, SUPPORTED_TYPES, DATAFUSION_FORMATS, false));
        }
        PROJECT_CAPS = caps;
    }

    @Override
    protected Set<FilterCapability> filterCapabilities() {
        return FILTER_CAPS;
    }

    @Override
    protected Set<ProjectCapability> projectCapabilities() {
        return PROJECT_CAPS;
    }

    @Override
    protected Set<AggregateCapability> aggregateCapabilities() {
        return AGG_CAPS;
    }

    // ---- SearchBackEndPlugin (storage) ----

    @Override
    public List<String> getSupportedFormats() {
        return List.of(PARQUET_DATA_FORMAT);
    }

    @Override
    public EngineReaderManager<Object> createReaderManager(ReaderManagerConfig settings) {
        return null;
    }
}

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
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;

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

    private static final Set<EngineCapability> OPERATOR_CAPS = Set.of(EngineCapability.SORT);

    private static final Set<FieldType> SUPPORTED_TYPES = new HashSet<>();
    static {
        SUPPORTED_TYPES.addAll(FieldType.numeric());
        SUPPORTED_TYPES.addAll(FieldType.keyword());
        SUPPORTED_TYPES.addAll(FieldType.date());
        SUPPORTED_TYPES.add(FieldType.BOOLEAN);
    }

    private static final Set<FilterOperator> STANDARD_OPS = Set.of(
        FilterOperator.EQUALS,
        FilterOperator.NOT_EQUALS,
        FilterOperator.GREATER_THAN,
        FilterOperator.GREATER_THAN_OR_EQUAL,
        FilterOperator.LESS_THAN,
        FilterOperator.LESS_THAN_OR_EQUAL,
        FilterOperator.IS_NULL,
        FilterOperator.IS_NOT_NULL,
        FilterOperator.IN,
        FilterOperator.LIKE
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
        for (FilterOperator op : STANDARD_OPS) {
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
        return bytes -> new ExchangeSink() {
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
    protected Set<ScanCapability> scanCapabilities() {
        return SCAN_CAPS;
    }

    @Override
    protected Set<FilterCapability> filterCapabilities() {
        return FILTER_CAPS;
    }

    @Override
    protected Set<AggregateCapability> aggregateCapabilities() {
        return AGG_CAPS;
    }

    // ---- SearchBackEndPlugin (storage) ----

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(new DataFormat() {
            @Override
            public String name() {
                return PARQUET_DATA_FORMAT;
            }

            @Override
            public long priority() {
                return 0;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
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
    public EngineReaderManager<Object> createReaderManager(ReaderManagerConfig settings) {
        return null;
    }
}

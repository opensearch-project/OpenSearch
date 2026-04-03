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
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.POINT_RANGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.STORED_FIELDS;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mock Lucene backend for tests. Supports lucene format with index structures
 * (full-text, point range) and stored fields. No columnar storage (doc values
 * only in parquet for default tests). Standard + full-text filter capabilities.
 * SCAN + FILTER only (no AGGREGATE).
 */
public class MockLuceneBackend implements AnalyticsSearchBackendPlugin {

    public static final String NAME = "mock-lucene";
    public static final String LUCENE_DATA_FORMAT = "lucene";
    private static final Set<String> LUCENE_FORMATS = Set.of(LUCENE_DATA_FORMAT);

    private static final Set<OperatorCapability> OPERATOR_CAPS = Set.of(
        OperatorCapability.SCAN, OperatorCapability.FILTER
    );

    private static final Set<FilterOperator> STANDARD_OPS = Set.of(
        FilterOperator.EQUALS, FilterOperator.NOT_EQUALS,
        FilterOperator.GREATER_THAN, FilterOperator.GREATER_THAN_OR_EQUAL,
        FilterOperator.LESS_THAN, FilterOperator.LESS_THAN_OR_EQUAL,
        FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL,
        FilterOperator.IN, FilterOperator.LIKE
    );

    private static final Set<FilterOperator> FULL_TEXT_OPS = Set.of(
        FilterOperator.MATCH, FilterOperator.MATCH_PHRASE,
        FilterOperator.MATCH_PHRASE_PREFIX, FilterOperator.MATCH_BOOL_PREFIX,
        FilterOperator.MULTI_MATCH, FilterOperator.QUERY_STRING,
        FilterOperator.SIMPLE_QUERY_STRING, FilterOperator.FUZZY,
        FilterOperator.WILDCARD, FilterOperator.REGEXP
    );

    private static final Set<FieldType> STANDARD_TYPES = new HashSet<>();
    static {
        STANDARD_TYPES.addAll(FieldType.numeric());
        STANDARD_TYPES.addAll(FieldType.keyword());
        STANDARD_TYPES.addAll(FieldType.text());
        STANDARD_TYPES.addAll(FieldType.date());
        STANDARD_TYPES.add(FieldType.BOOLEAN);
    }

    private static final Set<FieldType> FULL_TEXT_TYPES = new HashSet<>();
    static {
        FULL_TEXT_TYPES.addAll(FieldType.keyword());
        FULL_TEXT_TYPES.addAll(FieldType.text());
    }

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (FilterOperator op : STANDARD_OPS) {
            for (FieldType type : STANDARD_TYPES) {
                caps.add(new FilterCapability.Standard(op, type, LUCENE_FORMATS));
            }
        }
        for (FilterOperator op : FULL_TEXT_OPS) {
            for (FieldType type : FULL_TEXT_TYPES) {
                caps.add(new FilterCapability.FullText(op, type, LUCENE_FORMATS, Set.of()));
            }
        }
        FILTER_CAPS = caps;
    }

    @Override public String name() { return NAME; }

    @Override public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) { return null; }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(new DataFormat() {
            @Override public String name() { return LUCENE_DATA_FORMAT; }
            @Override public long priority() { return 0; }
            @Override public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of(
                    new FieldTypeCapabilities("integer", Set.of(POINT_RANGE, STORED_FIELDS)),
                    new FieldTypeCapabilities("long", Set.of(POINT_RANGE, STORED_FIELDS)),
                    new FieldTypeCapabilities("keyword", Set.of(FULL_TEXT_SEARCH, STORED_FIELDS)),
                    new FieldTypeCapabilities("text", Set.of(FULL_TEXT_SEARCH, STORED_FIELDS)),
                    new FieldTypeCapabilities("boolean", Set.of(STORED_FIELDS)),
                    new FieldTypeCapabilities("date", Set.of(POINT_RANGE, STORED_FIELDS))
                );
            }
        });
    }

    @Override public Set<OperatorCapability> supportedOperators() { return OPERATOR_CAPS; }
    @Override public Set<FilterCapability> filterCapabilities() { return FILTER_CAPS; }
}

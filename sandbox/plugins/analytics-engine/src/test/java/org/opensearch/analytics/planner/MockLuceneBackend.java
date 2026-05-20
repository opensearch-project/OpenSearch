/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mock Lucene backend for tests. Supports lucene format with index structures
 * (full-text, point range) and stored fields. Standard + full-text filter capabilities.
 * SCAN + FILTER only (no AGGREGATE).
 *
 * <p>Tests override only the capability methods they need — everything else
 * falls through to the defaults declared here.
 */
public class MockLuceneBackend extends MockBackend implements SearchBackEndPlugin<Object> {

    public static final String NAME = "mock-lucene";
    public static final String LUCENE_DATA_FORMAT = "lucene";
    private static final Set<String> LUCENE_FORMATS = Set.of(LUCENE_DATA_FORMAT);

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

    private static final Set<ScalarFunction> FULL_TEXT_OPS = Set.of(
        ScalarFunction.MATCH,
        ScalarFunction.MATCH_PHRASE,
        ScalarFunction.FUZZY,
        ScalarFunction.WILDCARD,
        ScalarFunction.REGEXP
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
        for (ScalarFunction op : STANDARD_OPS) {
            caps.add(new FilterCapability.Standard(op, STANDARD_TYPES, LUCENE_FORMATS));
        }
        for (ScalarFunction op : FULL_TEXT_OPS) {
            for (FieldType type : FULL_TEXT_TYPES) {
                caps.add(new FilterCapability.FullText(op, type, LUCENE_FORMATS, Set.of()));
            }
        }
        FILTER_CAPS = caps;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected Set<FilterCapability> filterCapabilities() {
        return FILTER_CAPS;
    }

    // ---- SearchBackEndPlugin (storage) ----

    @Override
    public List<String> getSupportedFormats() {
        return List.of(LUCENE_DATA_FORMAT);
    }

    @Override
    public EngineReaderManager<Object> createReaderManager(ReaderManagerConfig settings) {
        return null;
    }
}

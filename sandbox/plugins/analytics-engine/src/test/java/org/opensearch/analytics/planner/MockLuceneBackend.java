/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.FieldReferenceExtractor;
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
        ScalarFunction.REGEXP,
        ScalarFunction.QUERY_STRING
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

    /**
     * Default field-reference extractors for the multi-field full-text functions, mirroring the
     * planner's pre-extractor behavior: literals come from the {@code fields} MAP, fan-out when
     * empty, non-lenient. Tests needing in-string fields or explicit lenient override this.
     */
    @Override
    protected Map<ScalarFunction, FieldReferenceExtractor> fieldReferenceExtractors() {
        FieldReferenceExtractor mapLiteralExtractor = (call, fieldStorage) -> {
            List<String> literals = extractLiteralFieldNames(call);
            return new FieldReferences(literals, List.of(), false);
        };
        return Map.of(
            ScalarFunction.QUERY_STRING,
            mapLiteralExtractor,
            ScalarFunction.SIMPLE_QUERY_STRING,
            mapLiteralExtractor,
            ScalarFunction.MULTI_MATCH,
            mapLiteralExtractor
        );
    }

    /**
     * Extracts literal field names from the {@code fields}/{@code field} MAP operand of a
     * multi-field full-text {@code RexCall} — the field-name encoding the SQL plugin emits. Mirrors
     * the planner's pre-extractor behavior so existing full-text tests keep their semantics.
     */
    private static List<String> extractLiteralFieldNames(RexCall predicate) {
        List<String> names = new ArrayList<>();
        for (RexNode operand : predicate.getOperands()) {
            if (operand instanceof RexCall outerMap && outerMap.getOperands().size() >= 2) {
                RexNode keyNode = outerMap.getOperands().get(0);
                if (!(keyNode instanceof RexLiteral keyLit)) continue;
                String key = keyLit.getValueAs(String.class);
                if (!"fields".equals(key) && !"field".equals(key)) continue;

                RexNode valueNode = outerMap.getOperands().get(1);
                if (valueNode instanceof RexCall nestedMap) {
                    List<RexNode> nestedOperands = nestedMap.getOperands();
                    for (int i = 0; i + 1 < nestedOperands.size(); i += 2) {
                        if (nestedOperands.get(i) instanceof RexLiteral fieldLit) {
                            String fieldName = fieldLit.getValueAs(String.class);
                            if (fieldName != null && !fieldName.isEmpty()) {
                                names.add(fieldName);
                            }
                        }
                    }
                } else if (valueNode instanceof RexLiteral valueLit) {
                    String fieldName = valueLit.getValueAs(String.class);
                    if (fieldName != null && !fieldName.isEmpty()) {
                        names.add(fieldName);
                    }
                }
            }
        }
        return names;
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

    @Override
    public DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
        return (subtree, fieldStorage) -> {
            // Simple test convertor: walks the subtree and produces a descriptive string
            return describeSubtree(subtree).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        };
    }

    private static String describeSubtree(org.apache.calcite.rex.RexNode node) {
        if (node instanceof org.opensearch.analytics.planner.rel.AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof org.apache.calcite.rex.RexCall call) {
            switch (call.getKind()) {
                case AND:
                case OR:
                case NOT: {
                    java.util.List<String> children = new java.util.ArrayList<>();
                    for (org.apache.calcite.rex.RexNode child : call.getOperands()) {
                        children.add(describeSubtree(child));
                    }
                    return call.getKind() + "(" + String.join(",", children) + ")";
                }
                default:
                    return call.getOperator().getName();
            }
        }
        return node.toString();
    }
}

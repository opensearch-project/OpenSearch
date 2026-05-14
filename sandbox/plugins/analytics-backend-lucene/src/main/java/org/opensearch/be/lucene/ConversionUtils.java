/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reusable utilities for extracting fields and values from PPL relevance function
 * RexCall structures and serializing QueryBuilders.
 *
 * <p>PPL relevance functions encode arguments as MAP_VALUE_CONSTRUCTOR pairs:
 * {@code func(MAP('field', $ref), MAP('query', literal), [MAP('param', literal)]...)}
 * Each MAP has exactly 2 operands: key at index 0, value at index 1.
 */
final class ConversionUtils {

    /** MAP key for single-field relevance operands. */
    static final String KEY_FIELD = "field";
    /** MAP key for multi-field relevance operands. */
    static final String KEY_FIELDS = "fields";
    /** MAP key for the query text operand. */
    static final String KEY_QUERY = "query";

    private ConversionUtils() {}

    /**
     * Extracts field name from a MAP_VALUE_CONSTRUCTOR operand: MAP('field', $inputRef).
     */
    static String extractFieldFromRelevanceMap(RexCall call, int operandIndex, List<FieldStorageInfo> fieldStorage) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall) {
            RexNode value = mapCall.getOperands().get(1);
            if (value instanceof RexInputRef inputRef) {
                return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
            }
        }
        if (operand instanceof RexInputRef inputRef) {
            return FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName();
        }
        throw new IllegalArgumentException("Cannot extract field name from operand " + operandIndex + ": " + operand);
    }

    /**
     * Extracts string value from a MAP_VALUE_CONSTRUCTOR operand: MAP('key', 'value').
     */
    static String extractStringFromRelevanceMap(RexCall call, int operandIndex) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall) {
            RexNode value = mapCall.getOperands().get(1);
            if (value instanceof RexLiteral literal) {
                return literal.getValueAs(String.class);
            }
        }
        if (operand instanceof RexLiteral literal) {
            return literal.getValueAs(String.class);
        }
        throw new IllegalArgumentException("Cannot extract string from operand " + operandIndex + ": " + operand);
    }

    /**
     * Serializes a QueryBuilder into bytes using NamedWriteable protocol.
     */
    static byte[] serializeQueryBuilder(QueryBuilder queryBuilder) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(queryBuilder);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to serialize delegated query: " + queryBuilder, exception);
        }
    }

    /**
     * Extracts the key string from a MAP_VALUE_CONSTRUCTOR operand: MAP('key', value).
     * Returns null if the operand is not a MAP or the key is not a string literal.
     */
    static String extractMapKey(RexCall call, int operandIndex) {
        RexNode operand = call.getOperands().get(operandIndex);
        if (operand instanceof RexCall mapCall && mapCall.getOperands().size() >= 2) {
            RexNode key = mapCall.getOperands().get(0);
            if (key instanceof RexLiteral literal) {
                return literal.getValueAs(String.class);
            }
        }
        return null;
    }

    /**
     * Extracted operands from a relevance function RexCall.
     * @param fieldName  single field name (null if not present or multi-field)
     * @param fields     multiple field names (null if not present)
     * @param query      the query string (null if not found)
     */
    record RelevanceOperands(String fieldName, List<String> fields, String query) {
    }

    /**
     * Extracts field/fields and query from a relevance function RexCall by MAP key lookup,
     * with positional fallback for non-MAP operand structures (e.g. MATCH($ref, literal)).
     *
     * @param call         the relevance function RexCall
     * @param fieldStorage per-column storage metadata for resolving field names
     * @return extracted operands
     */
    static RelevanceOperands extractRelevanceOperands(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = null;
        List<String> fields = null;
        String query = null;

        for (int i = 0; i < call.getOperands().size(); i++) {
            String key = extractMapKey(call, i);
            if (KEY_FIELD.equals(key)) {
                fieldName = extractFieldFromRelevanceMap(call, i, fieldStorage);
            } else if (KEY_FIELDS.equals(key)) {
                fields = extractFieldsFromRelevanceMap(call, i, fieldStorage);
            } else if (KEY_QUERY.equals(key)) {
                query = extractStringFromRelevanceMap(call, i);
            }
        }

        // Fallback: positional extraction for non-MAP operand structures (e.g. MATCH($ref, literal))
        if (fieldName == null && fields == null && query == null && call.getOperands().size() >= 2) {
            fieldName = extractFieldFromRelevanceMap(call, 0, fieldStorage);
            query = extractStringFromRelevanceMap(call, 1);
        }

        return new RelevanceOperands(fieldName, fields, query);
    }

    /**
     * Extracts multiple field names from a MAP_VALUE_CONSTRUCTOR operand
     * for multi-field full-text functions (multi_match, query_string, simple_query_string).
     *
     * <p>The operand structure for multi-field functions:
     * {@code MAP('fields', MAP('field1':VARCHAR, boost1:DOUBLE, 'field2':VARCHAR, boost2:DOUBLE, ...))}
     * The outer MAP has key='fields' at index 0 and a nested MAP at index 1.
     * The nested MAP is a Calcite MAP_VALUE_CONSTRUCTOR with strict alternating key-value pairs:
     * field name (VARCHAR) at even indices, boost value (DOUBLE) at odd indices.
     *
     * <p>Also supports the RexInputRef-based structure for single-field fallback:
     * {@code MAP('field', $ref1, 'field', $ref2, ...)}
     *
     * <p>Note: This method is intentionally not recursive. The MAP nesting depth is bounded
     * to at most 2 levels by Calcite's MAP_VALUE_CONSTRUCTOR design: an outer MAP holding
     * the 'fields' key and a nested MAP holding field-name/boost pairs. Deeper nesting does
     * not occur in the PPL relevance function encoding.
     *
     * <p>TODO: extract per-field boost values and return them alongside field names.
     */
    static List<String> extractFieldsFromRelevanceMap(RexCall call, int operandIndex, List<FieldStorageInfo> fieldStorage) {
        RexNode operand = call.getOperands().get(operandIndex);
        List<String> fields = new ArrayList<>();
        if (operand instanceof RexCall outerMapCall) {
            // Check if the value (index 1) is a nested MAP containing field name/boost pairs
            if (outerMapCall.getOperands().size() >= 2) {
                RexNode value = outerMapCall.getOperands().get(1);
                if (value instanceof RexCall nestedMapCall) {
                    // Nested MAP: strict alternating key-value pairs from MAP_VALUE_CONSTRUCTOR.
                    // Even indices (0, 2, 4...) are field name VARCHAR literals.
                    // Odd indices (1, 3, 5...) are boost DOUBLE literals (ignored for now).
                    List<RexNode> nestedOperands = nestedMapCall.getOperands();
                    for (int i = 0; i < nestedOperands.size(); i += 2) {
                        RexNode fieldNode = nestedOperands.get(i);
                        if (fieldNode instanceof RexLiteral fieldLiteral) {
                            fields.add(fieldLiteral.getValueAs(String.class));
                        }
                    }
                    if (fields.isEmpty() == false) {
                        return fields;
                    }
                }
            }
            // Fallback: RexInputRef-based structure MAP('field', $ref1, 'field', $ref2, ...)
            List<RexNode> mapOperands = outerMapCall.getOperands();
            for (int i = 1; i < mapOperands.size(); i += 2) {
                RexNode val = mapOperands.get(i);
                if (val instanceof RexInputRef inputRef) {
                    fields.add(FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName());
                }
            }
        } else if (operand instanceof RexInputRef inputRef) {
            fields.add(FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex()).getFieldName());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Cannot extract field names from operand " + operandIndex + ": " + operand);
        }
        return fields;
    }
}

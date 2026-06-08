/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Validates that explicitly-named fields in multi-field text-relevance functions are mapped as
 * {@code text} or {@code keyword}, and extracts literal field names from the MAP operand structure
 * those functions use to encode their field list.
 *
 * <p>Multi-field full-text functions ({@code multi_match}, {@code query_string},
 * {@code simple_query_string}) don't reference their fields through {@link
 * org.apache.calcite.rex.RexInputRef}; instead they encode field names as string literals inside a
 * nested {@code MAP_VALUE_CONSTRUCTOR}. This class extracts those literal field names and rejects
 * any that resolve to a non-text/keyword mapping so users get a precise, actionable planning error
 * (naming the field and its type) instead of a generic "no backend can evaluate" failure later in
 * the pipeline.
 *
 * <p>Extracted from {@link OpenSearchFilterRule} to keep that rule focused on predicate annotation
 * and viable-backend computation.
 *
 * @opensearch.internal
 */
public final class TextRelevanceFieldValidator {

    /**
     * The multi-field/literal-MAP text-relevance functions that encode their field list as string
     * literals in a nested MAP. Single-field {@code MATCH} variants are intentionally excluded —
     * they reference their field through {@link org.apache.calcite.rex.RexInputRef} and so flow
     * through the standard field-index path instead.
     */
    private static final Set<ScalarFunction> LITERAL_FIELD_TEXT_FUNCTIONS = Set.of(
        ScalarFunction.QUERY_STRING,
        ScalarFunction.SIMPLE_QUERY_STRING,
        ScalarFunction.MULTI_MATCH
    );

    /**
     * Field types a text-relevance function may be applied to: {@code text} family ∪ {@code keyword}
     * family. Built once at class init from {@link FieldType#text()} and {@link FieldType#keyword()}
     * rather than per validation call.
     */
    private static final Set<FieldType> ALLOWED_TEXT_FIELD_TYPES;

    static {
        EnumSet<FieldType> allowed = EnumSet.noneOf(FieldType.class);
        allowed.addAll(FieldType.text());
        allowed.addAll(FieldType.keyword());
        ALLOWED_TEXT_FIELD_TYPES = allowed;
    }

    private TextRelevanceFieldValidator() {
        // utility class
    }

    /**
     * Returns whether {@code function} encodes its field list as string literals in a nested MAP
     * (i.e. is one of {@code query_string}, {@code simple_query_string}, {@code multi_match}). Only
     * for these functions should {@link #extractLiteralFieldNames} and {@link
     * #rejectNonTextFieldsForTextFunction} be applied; other full-text functions either reference
     * fields via {@code RexInputRef} or carry no explicit field list.
     */
    public static boolean usesLiteralFieldEncoding(ScalarFunction function) {
        return LITERAL_FIELD_TEXT_FUNCTIONS.contains(function);
    }

    /**
     * Extracts field names from the literal MAP structure of multi-field full-text RexCalls.
     *
     * <p>Multi-field functions like {@code query_string(['severityNumber'], 'severityNumber:>15')}
     * encode field names as string literals in a nested MAP_VALUE_CONSTRUCTOR rather than as
     * {@link org.apache.calcite.rex.RexInputRef}. The structure is:
     * <pre>
     *   func(
     *     MAP('fields', MAP('field1':VARCHAR, boost1:DOUBLE, 'field2':VARCHAR, boost2:DOUBLE, ...)),
     *     MAP('query', '...':VARCHAR)
     *   )
     * </pre>
     * This method walks the call tree to find all string literals that represent field names —
     * specifically, RexLiteral children at even indices of a nested MAP whose outer key is "fields"
     * or "field". Returns an empty list if no such names are found (e.g. zero-field functions
     * like MATCHALL or QUERY without an explicit field list).
     *
     * <p>TODO: Push this extraction down into the FullText function definition itself so the operand layout is
     * owned in one spot and the planner just asks the function for its named fields rather than
     * re-parsing the RexCall here.
     */
    public static List<String> extractLiteralFieldNames(RexCall predicate) {
        List<String> names = new ArrayList<>();
        for (RexNode operand : predicate.getOperands()) {
            if (operand instanceof RexCall outerMap && outerMap.getOperands().size() >= 2) {
                // Outer MAP: MAP('key', value). Check if the key is "fields" or "field".
                RexNode keyNode = outerMap.getOperands().get(0);
                if (!(keyNode instanceof RexLiteral keyLit)) continue;
                String key = keyLit.getValueAs(String.class);
                if (!"fields".equals(key) && !"field".equals(key)) continue;

                RexNode valueNode = outerMap.getOperands().get(1);
                if (valueNode instanceof RexCall nestedMap) {
                    // Nested MAP with field-name/boost pairs at even/odd indices.
                    List<RexNode> nestedOperands = nestedMap.getOperands();
                    // Guard against odd sizes: only read complete (field name at i, boost at i+1) pairs.
                    for (int i = 0; i + 1 < nestedOperands.size(); i += 2) {
                        RexNode fieldNode = nestedOperands.get(i);
                        if (fieldNode instanceof RexLiteral fieldLit) {
                            String fieldName = fieldLit.getValueAs(String.class);
                            if (fieldName != null && !fieldName.isEmpty()) {
                                names.add(fieldName);
                            }
                        }
                    }
                } else if (valueNode instanceof RexLiteral valueLit) {
                    // Single-field shape: MAP('field', 'fieldName').
                    String fieldName = valueLit.getValueAs(String.class);
                    if (fieldName != null && !fieldName.isEmpty()) {
                        names.add(fieldName);
                    }
                }
            }
        }
        return names;
    }

    /**
     * Validates that all explicitly named fields used in a text-relevance function
     * (e.g. {@code query_string}, {@code simple_query_string}, {@code multi_match})
     * are mapped as {@code text} or {@code keyword}. Throws a descriptive
     * {@link IllegalArgumentException} naming the offending field, its mapping type,
     * and the function — surfaced at planning so users get a clear, actionable error
     * instead of a generic "no backend can evaluate" failure later in the pipeline.
     *
     * <p>Fields whose storage info cannot be resolved (unknown columns, dynamic
     * mappings) are skipped — they fall through to the existing capability-based
     * matching where they get the conservative TEXT-type assumption.
     */
    public static void rejectNonTextFieldsForTextFunction(
        String functionName,
        List<String> fieldNames,
        List<FieldStorageInfo> fieldStorageInfos
    ) {
        for (String fieldName : fieldNames) {
            FieldStorageInfo storageInfo = findStorageByFieldName(fieldStorageInfos, fieldName);
            if (storageInfo == null) {
                continue;
            }
            FieldType type = storageInfo.getFieldType();
            if (type != null && ALLOWED_TEXT_FIELD_TYPES.contains(type) == false) {
                String mappingType = storageInfo.getMappingType() != null ? storageInfo.getMappingType() : "unknown";
                throw new IllegalArgumentException(
                    "Text-relevance function ["
                        + functionName
                        + "] cannot be applied to field ["
                        + fieldName
                        + "] of type ["
                        + mappingType
                        + "]. Only text and keyword fields are supported. "
                        + "Use a typed comparison (e.g. ["
                        + fieldName
                        + " > value]) for non-text fields."
                );
            }
        }
    }

    /**
     * Looks up a {@link FieldStorageInfo} by field name. Returns null if no match.
     */
    private static FieldStorageInfo findStorageByFieldName(List<FieldStorageInfo> fieldStorageInfos, String fieldName) {
        for (FieldStorageInfo info : fieldStorageInfos) {
            if (fieldName.equals(info.getFieldName())) {
                return info;
            }
        }
        return null;
    }
}

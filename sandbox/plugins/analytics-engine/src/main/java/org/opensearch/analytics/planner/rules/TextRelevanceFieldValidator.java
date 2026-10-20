/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Validates that explicitly-named fields in multi-field text-relevance functions are mapped as
 * {@code text} or {@code keyword}.
 *
 * <p>Multi-field full-text functions ({@code multi_match}, {@code query_string},
 * {@code simple_query_string}) encode field names as string literals inside a nested
 * {@code MAP_VALUE_CONSTRUCTOR} rather than through {@link org.apache.calcite.rex.RexInputRef}.
 * The backend's {@code DelegatedPredicateSerializer.referencedFields} surfaces those names; this class rejects any that
 * resolve to a non-text/keyword mapping so users get a precise, actionable planning error (naming
 * the field and its type) instead of a generic "no backend can evaluate" failure later in the
 * pipeline.
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
     * for these functions should {@link #rejectNonTextFieldsForTextFunction} be applied; other
     * full-text functions either reference fields via {@code RexInputRef} or carry no explicit
     * field list.
     */
    public static boolean usesLiteralFieldEncoding(ScalarFunction function) {
        return LITERAL_FIELD_TEXT_FUNCTIONS.contains(function);
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

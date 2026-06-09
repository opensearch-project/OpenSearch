/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.List;

/**
 * The fields a relevance predicate (e.g. {@code query_string}, {@code simple_query_string},
 * {@code multi_match}) explicitly references, plus the metadata the planner needs to decide whether
 * to eagerly validate them.
 *
 * <p>Produced by a {@link FieldReferenceExtractor} at planning time from the predicate's
 * {@link org.apache.calcite.rex.RexCall} and consumed by the planner's text-relevance validation.
 * Field-name extraction is analyzer-independent, so this can be computed without a
 * {@code QueryShardContext}.
 *
 * <p>The planner validates only {@link #literalFields()}. {@link #patternTokens()} and
 * {@link #defaultFieldFanout()} are informational and pass through unvalidated — wildcard/regex
 * field expansion and default-field fan-out are resolved at execution by the data node, which
 * matches OpenSearch's best-effort, never-erroring treatment of those forms.
 *
 * @param literalFields      explicitly-named concrete field names (from the {@code fields}/{@code field}
 *                           operand and, for {@code query_string}, literal {@code field:} tokens in the
 *                           query string). Validated by the planner. First-appearance ordered.
 * @param patternTokens      explicitly-named wildcard/regex tokens (e.g. {@code cat*}, {@code *}),
 *                           classified via {@code Regex.isSimpleMatchPattern}. Passed through — not
 *                           expanded, not rejected.
 * @param defaultFieldFanout {@code true} when the query has terms with no field qualifier, so
 *                           OpenSearch fans them out to {@code default_field}/all queryable fields.
 *                           Passed through (assume text, no rejection).
 * @param lenient            effective lenient flag: the explicit {@code lenient} param when set,
 *                           otherwise {@code true} (assume tolerant when unset). When {@code true} the
 *                           planner suppresses eager type rejection of {@link #literalFields()}.
 *
 * @opensearch.internal
 */
public record FieldReferences(List<String> literalFields, List<String> patternTokens, boolean defaultFieldFanout, boolean lenient) {

    /**
     * Normalizes null token lists to empty and wraps both in unmodifiable lists so the result is a
     * safe, immutable value object.
     */
    public FieldReferences {
        literalFields = literalFields == null ? List.of() : List.copyOf(literalFields);
        patternTokens = patternTokens == null ? List.of() : List.copyOf(patternTokens);
    }
}

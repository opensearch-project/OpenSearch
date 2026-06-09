/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.analytics.spi.FieldReferenceExtractor;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.Map;

/**
 * Registry of per-function {@link FieldReferenceExtractor}s for the multi-field relevance functions.
 * Consumed by the planner (via {@code BackendCapabilityProvider.fieldReferenceExtractors()}) to
 * validate the fields these functions reference, including fields written inside a
 * {@code query_string} body.
 *
 * <p>Only {@code query_string} parses its query string for in-string fields; {@code simple_query_string}
 * and {@code multi_match} reference fields solely through their {@code fields} operand.
 */
final class FieldReferenceExtractorRegistry {

    private static final Map<ScalarFunction, FieldReferenceExtractor> EXTRACTORS = Map.of(
        ScalarFunction.QUERY_STRING,
        new LuceneFieldReferenceExtractor("query_string", true),
        ScalarFunction.SIMPLE_QUERY_STRING,
        new LuceneFieldReferenceExtractor("simple_query_string", false),
        ScalarFunction.MULTI_MATCH,
        new LuceneFieldReferenceExtractor("multi_match", false)
    );

    private FieldReferenceExtractorRegistry() {}

    static Map<ScalarFunction, FieldReferenceExtractor> getExtractors() {
        return EXTRACTORS;
    }
}

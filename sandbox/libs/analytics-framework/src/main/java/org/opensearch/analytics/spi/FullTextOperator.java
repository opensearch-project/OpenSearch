/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Full-text search operations that an inverted index backend may support.
 * Intended as a common facade between Lucene, Tantivy, and future
 * full-text backends.
 *
 * @opensearch.internal
 */
public enum FullTextOperator {
    MATCH,
    MATCH_PHRASE,
    MATCH_PHRASE_PREFIX,
    MATCH_BOOL_PREFIX,
    MULTI_MATCH,
    QUERY_STRING,
    SIMPLE_QUERY_STRING,
    FUZZY,
    SPAN_NEAR,
    SPAN_OR,
    SPAN_NOT,
    SPAN_FIRST,
    SPAN_TERM
}

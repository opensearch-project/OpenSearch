/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene field implementations for text-based OpenSearch types.
 *
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.text.TextLuceneField} — Full-text fields
 *       indexed with positions for phrase queries and BM25 scoring.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.text.KeywordLuceneField} — Keyword (exact match)
 *       fields with {@code SortedSetDocValuesField} for sorting/aggregations.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.text.IpLuceneField} — IP address fields using
 *       {@code InetAddressPoint} for range queries.</li>
 * </ul>
 */
package org.opensearch.be.lucene.fields.core.data.text;

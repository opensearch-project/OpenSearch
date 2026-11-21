/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene field implementations for boolean and binary OpenSearch data types.
 *
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.BooleanLuceneField} — Indexed as "T"/"F"
 *       with {@code SortedNumericDocValuesField} for sorting/aggregations.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.BinaryLuceneField} — Stored as a
 *       {@code StoredField} for raw byte retrieval.</li>
 * </ul>
 */
package org.opensearch.be.lucene.fields.core.data;

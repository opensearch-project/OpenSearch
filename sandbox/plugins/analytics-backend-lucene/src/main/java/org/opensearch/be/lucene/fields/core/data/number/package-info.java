/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene field implementations for all OpenSearch numeric types.
 *
 * <p>Each class maps an OpenSearch numeric type to its corresponding Lucene point field
 * for range queries and {@code SortedNumericDocValuesField} for sorting/aggregations.
 *
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.ByteLuceneField} — 8-bit signed ({@code IntPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.ShortLuceneField} — 16-bit signed ({@code IntPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.IntegerLuceneField} — 32-bit signed ({@code IntPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.LongLuceneField} — 64-bit signed ({@code LongPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.UnsignedLongLuceneField} — 64-bit unsigned ({@code BigIntegerPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.HalfFloatLuceneField} — 16-bit float ({@code HalfFloatPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.FloatLuceneField} — 32-bit float ({@code FloatPoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.DoubleLuceneField} — 64-bit float ({@code DoublePoint})</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number.TokenCountLuceneField} — Token count stored as 32-bit integer</li>
 * </ul>
 */
package org.opensearch.be.lucene.fields.core.data.number;

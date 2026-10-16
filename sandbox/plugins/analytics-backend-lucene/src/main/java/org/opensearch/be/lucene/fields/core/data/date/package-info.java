/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene field implementations for date and timestamp OpenSearch types.
 *
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.date.DateLuceneField} — Millisecond-precision
 *       timestamps using {@code LongPoint} and {@code SortedNumericDocValuesField}.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.date.DateNanosLuceneField} — Nanosecond-precision
 *       timestamps using {@code LongPoint} and {@code SortedNumericDocValuesField}.</li>
 * </ul>
 */
package org.opensearch.be.lucene.fields.core.data.date;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet field implementations for date and timestamp OpenSearch types.
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.core.data.date.DateParquetField} — Millisecond-precision
 *       timestamps using Arrow {@code TimeStampMilliVector}.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.date.DateNanosParquetField} — Nanosecond-precision
 *       timestamps using Arrow {@code TimeStampNanoVector}.</li>
 * </ul>
 */
package org.opensearch.parquet.fields.core.data.date;

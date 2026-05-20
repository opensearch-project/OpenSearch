/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet field implementations for text-based OpenSearch types.
 *
 * <p>All implementations use Arrow {@code VarCharVector} with UTF-8 encoding.
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.core.data.text.TextParquetField} — Full-text fields.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.text.KeywordParquetField} — Keyword (exact match) fields.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.text.IpParquetField} — IP address fields stored as binary-encoded addresses.</li>
 * </ul>
 */
package org.opensearch.parquet.fields.core.data.text;

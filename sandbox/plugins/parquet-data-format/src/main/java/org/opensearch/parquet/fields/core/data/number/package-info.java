/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet field implementations for all OpenSearch numeric types.
 *
 * <p>Each class maps an OpenSearch numeric type to its corresponding Arrow integer or
 * floating-point vector. All implementations use {@code setSafe()} for bounds-checked writes.
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.ByteParquetField} — 8-bit signed ({@code TinyIntVector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.ShortParquetField} — 16-bit signed ({@code SmallIntVector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.IntegerParquetField} — 32-bit signed ({@code IntVector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.LongParquetField} — 64-bit signed ({@code BigIntVector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.UnsignedLongParquetField} — 64-bit unsigned ({@code UInt8Vector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.HalfFloatParquetField} — 16-bit float ({@code Float2Vector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.FloatParquetField} — 32-bit float ({@code Float4Vector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.DoubleParquetField} — 64-bit float ({@code Float8Vector})</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number.TokenCountParquetField} — Token count stored as 32-bit integer</li>
 * </ul>
 */
package org.opensearch.parquet.fields.core.data.number;

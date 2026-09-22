/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet field implementations for boolean and binary OpenSearch data types.
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.core.data.BooleanParquetField} — Maps to Arrow
 *       {@code BitVector}; stores boolean values as single-bit flags.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.BinaryParquetField} — Maps to Arrow
 *       {@code VarBinaryVector}; stores raw byte arrays.</li>
 * </ul>
 */
package org.opensearch.parquet.fields.core.data;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Parquet field implementations for OpenSearch metadata fields.
 *
 * <p>These fields handle internal OpenSearch document metadata that is stored alongside
 * user data in the Parquet file.
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.core.metadata.IdParquetField} — Document {@code _id},
 *       stored as binary ({@code VarBinaryVector}) from {@code byte[]}.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.metadata.RoutingParquetField} — Document {@code _routing},
 *       stored as UTF-8 text ({@code VarCharVector}).</li>
 *   <li>{@link org.opensearch.parquet.fields.core.metadata.IgnoredParquetField} — The {@code _ignored}
 *       field, stored as UTF-8 text ({@code VarCharVector}).</li>
 *   <li>{@link org.opensearch.parquet.fields.core.metadata.SizeParquetField} — Document {@code _size},
 *       stored as 32-bit integer ({@code IntVector}).</li>
 * </ul>
 */
package org.opensearch.parquet.fields.core.metadata;

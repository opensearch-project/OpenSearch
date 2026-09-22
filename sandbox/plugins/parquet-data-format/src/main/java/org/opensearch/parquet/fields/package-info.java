/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Field type mapping between OpenSearch and Apache Arrow for Parquet generation.
 *
 * <p>This package provides the extensible type system that maps OpenSearch {@code MappedFieldType}
 * instances to their corresponding Apache Arrow vector types. The mapping is used during document
 * ingestion to write field values into the correct Arrow vectors within a {@code VectorSchemaRoot}.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.ParquetField} — Abstract base class that all field
 *       type implementations extend. Defines the contract for Arrow type declaration and
 *       value writing into managed VSR vectors.</li>
 *   <li>{@link org.opensearch.parquet.fields.ArrowFieldRegistry} — Singleton registry mapping
 *       OpenSearch type names (e.g., {@code "integer"}, {@code "keyword"}) to their
 *       {@code ParquetField} implementations. Populated at class load via field plugins.</li>
 *   <li>{@link org.opensearch.parquet.fields.ArrowSchemaBuilder} — Utility that builds an Arrow
 *       {@code Schema} from a {@code MapperService}, filtering unsupported metadata fields
 *       and resolving each mapper to its registered Arrow type.</li>
 * </ul>
 *
 * <h2>Sub-packages</h2>
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.plugins} — Plugin interface and built-in registrations.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data} — Data field implementations (boolean, binary).</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.number} — Numeric field implementations.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.text} — Text-based field implementations.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.data.date} — Date/timestamp field implementations.</li>
 *   <li>{@link org.opensearch.parquet.fields.core.metadata} — OpenSearch metadata field implementations.</li>
 * </ul>
 *
 * @see org.opensearch.parquet.fields.ParquetField
 * @see org.opensearch.parquet.fields.ArrowFieldRegistry
 */
package org.opensearch.parquet.fields;

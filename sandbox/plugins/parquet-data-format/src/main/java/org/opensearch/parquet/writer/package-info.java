/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Document input and writer integration for the Parquet data format.
 *
 * <p>This package provides the writer-level abstraction that bridges OpenSearch's
 * {@code Writer} interface with the VSR-based batching layer. Documents are collected
 * as field-value pairs, then transferred to Arrow vectors by the VSR manager.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.parquet.writer.FieldValuePair} — Immutable pair of an OpenSearch
 *       {@code MappedFieldType} and its parsed value, representing a single field in a document.</li>
 *   <li>{@link org.opensearch.parquet.writer.ParquetDocumentInput} — Implements {@code DocumentInput},
 *       collecting field-value pairs for a single document. Cleared on {@code close()}.</li>
 *   <li>{@link org.opensearch.parquet.writer.ParquetWriter} — Implements {@code Writer},
 *       accepting documents via {@code addDoc()} and producing Parquet files on {@code flush()}.
 *       Delegates batching and native writing to {@link org.opensearch.parquet.vsr.VSRManager}.</li>
 * </ul>
 *
 * @see org.opensearch.parquet.writer.ParquetWriter
 * @see org.opensearch.parquet.writer.ParquetDocumentInput
 */
package org.opensearch.parquet.writer;

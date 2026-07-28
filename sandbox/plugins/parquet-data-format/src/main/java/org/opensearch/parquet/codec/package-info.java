/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Read-only Lucene {@code DocValuesFormat} codec that materializes per-document values from
 * Parquet files through the standard {@code DocValuesProducer} API, for flat indices
 * (Row ID = Doc ID).
 *
 * <p>This package and its subpackages hold the codec implementation: the physical-type
 * mapping ({@link org.opensearch.parquet.codec.ParquetPhysicalType}), the layered cache
 * structures ({@code org.opensearch.parquet.codec.cache}), and (in later tasks) the
 * DocValues iterators, ordinal table, producer, and format.
 */
package org.opensearch.parquet.codec;

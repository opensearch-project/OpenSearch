/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Read-only Lucene DocValues codec that serves per-document numeric values from Parquet files.
 *
 * <p>Fields that live only in Parquet have no Lucene segment {@code FieldInfo}, so the standard
 * {@code PerFieldDocValuesFormat} cannot route to them. The codec closes that gap with a reader
 * wrapper ({@code ParquetDocValuesDirectoryReader} -&gt; {@code ParquetDocValuesLeafReader}) that
 * synthesizes the missing {@code FieldInfo}s and serves their doc values through a
 * {@code ParquetDocValuesProducer}. The producer opens a forward-only native cursor per iterator
 * (see {@code org.opensearch.parquet.docvaluescodec.bridge}) and reads decoded batches in place.
 *
 * <p>Scope: single-valued numeric fields (byte, short, integer, long, float, double, date,
 * date_nanos). Binary, keyword, boolean, and genuinely multi-valued columns are not served yet.
 */
package org.opensearch.parquet.docvaluescodec;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Field plugin interface and built-in plugin registrations.
 *
 * <p>This package defines the {@link org.opensearch.parquet.fields.plugins.ParquetFieldPlugin}
 * interface used to register batches of field type mappings with the
 * {@link org.opensearch.parquet.fields.ArrowFieldRegistry}. Two built-in plugins are provided:
 *
 * <ul>
 *   <li>{@link org.opensearch.parquet.fields.plugins.CoreDataFieldPlugin} — Registers all
 *       standard OpenSearch data types: numeric (byte through unsigned_long), text, keyword,
 *       IP, boolean, binary, date, and date_nanos.</li>
 *   <li>{@link org.opensearch.parquet.fields.plugins.MetadataFieldPlugin} — Registers
 *       OpenSearch metadata fields: _id, _routing, _ignored, _size, _doc_count,
 *       _seq_no, and _version.</li>
 * </ul>
 *
 * @see org.opensearch.parquet.fields.plugins.ParquetFieldPlugin
 */
package org.opensearch.parquet.fields.plugins;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Field type mapping between OpenSearch and Lucene for document indexing.
 *
 * <p>This package provides the extensible type system that maps OpenSearch {@code MappedFieldType}
 * instances to their corresponding Lucene document fields. The mapping is used during document
 * ingestion to write field values into the correct Lucene field types within a {@code Document}.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.LuceneField} — Abstract base class that all field
 *       type implementations extend. Defines the contract for adding Lucene fields to a document
 *       based on the field's mapping configuration (searchable, doc values, stored).</li>
 * </ul>
 *
 * <h2>Sub-packages</h2>
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.plugins} — Plugin interface and built-in registrations.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data} — Data field implementations (boolean, binary).</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number} — Numeric field implementations.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.text} — Text-based field implementations.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.date} — Date/timestamp field implementations.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.metadata} — OpenSearch metadata field implementations.</li>
 * </ul>
 *
 * @see org.opensearch.be.lucene.fields.LuceneField
 * @see org.opensearch.be.lucene.fields.core.LuceneFieldRegistry
 */
package org.opensearch.be.lucene.fields;

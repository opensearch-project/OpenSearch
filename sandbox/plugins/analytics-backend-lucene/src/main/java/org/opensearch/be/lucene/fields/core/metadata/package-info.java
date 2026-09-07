/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene field implementations for OpenSearch metadata fields.
 *
 * <p>These fields handle internal OpenSearch document metadata that is stored alongside
 * user data in the Lucene index.
 *
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.core.metadata.IdLuceneField} — Document {@code _id},
 *       indexed as a {@code StringField} from Lucene {@code BytesRef}.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.metadata.RoutingLuceneField} — Document {@code _routing},
 *       indexed as a stored {@code StringField}.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.metadata.IgnoredLuceneField} — The {@code _ignored}
 *       field, indexed as a {@code StringField}.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.metadata.SizeLuceneField} — Document {@code _size},
 *       stored as {@code IntPoint} with {@code SortedNumericDocValuesField}.</li>
 * </ul>
 */
package org.opensearch.be.lucene.fields.core.metadata;

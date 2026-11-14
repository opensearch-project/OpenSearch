/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Core built-in Lucene field implementations organized by data category.
 *
 * <p>This is an organizational parent package. Concrete field implementations
 * are in the following sub-packages:
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data} — Boolean and binary fields.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.number} — All numeric types.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.text} — Text, keyword, and IP fields.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.data.date} — Date and date_nanos fields.</li>
 *   <li>{@link org.opensearch.be.lucene.fields.core.metadata} — OpenSearch metadata fields.</li>
 * </ul>
 */
package org.opensearch.be.lucene.fields.core;

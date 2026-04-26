/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene indexing engine implementation for the pluggable data format framework.
 *
 * <p>This package contains the core components that enable Lucene to participate as a
 * data format in the composite engine alongside Parquet:
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.index.LuceneWriter} — per-generation writer with
 *       isolated IndexWriter, force-merge to 1 segment, and row ID tracking</li>
 *   <li>{@link org.opensearch.be.lucene.index.LuceneIndexingExecutionEngine} — engine that
 *       creates writers and incorporates flushed segments into the shared committer via addIndexes</li>
 *   <li>{@link org.opensearch.be.lucene.index.LuceneCommitter} — owns the shared IndexWriter
 *       for durable commits of the catalog snapshot</li>
 *   <li>{@link org.opensearch.be.lucene.index.LuceneDocumentInput} — builds Lucene Documents
 *       from OpenSearch field types using the field factory registry</li>
 *   <li>{@link org.opensearch.be.lucene.index.LuceneWriterCodec} — codec wrapper that injects
 *       writer generation as a segment info attribute for post-addIndexes correlation</li>
 * </ul>
 */
package org.opensearch.be.lucene.index;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Lucene merge implementation for the composite engine using {@code addIndexes(CodecReader...)}
 * with IndexSort-based document reordering.
 *
 * <h2>How it works</h2>
 *
 * <ul>
 *   <li><b>Value rewriting</b> — Each source CodecReader is wrapped with
 *       {@link org.opensearch.be.lucene.merge.RowIdRemappingCodecReader} which replaces
 *       {@code ___row_id} doc values with the remapped global values from the RowIdMapping.</li>
 *   <li><b>Document ordering</b> — {@code addIndexes(CodecReader...)} applies the writer's
 *       IndexSort from scratch (full sort, not merge-sort). The standard
 *       {@link org.opensearch.be.lucene.merge.RowIdRemappingSortField} reads the already-remapped
 *       values and sorts all documents by ascending {@code ___row_id}, including cross-segment
 *       interleaving and within-segment reordering.</li>
 *   <li><b>Segment cleanup</b> — Lucene's internal merge path handles segment lifecycle:
 *       {@code commitMerge} removes old segments from the live list and decrements file references.</li>
 * </ul>
 *
 * <h2>Key classes</h2>
 * <ul>
 *   <li>{@link org.opensearch.be.lucene.merge.LuceneMerger} — Orchestrates the merge.</li>
 *   <li>{@link org.opensearch.be.lucene.merge.RowIdRemappingSortField} — SortField on
 *       {@code ___row_id} for IndexSort-based reordering.</li>
 *   <li>{@link org.opensearch.be.lucene.merge.RowIdRemappingCodecReader} — FilterCodecReader
 *       that remaps {@code ___row_id} doc values.</li>
 *   <li>{@link org.opensearch.be.lucene.merge.RowIdRemappingDocValuesProducer} — DocValuesProducer
 *       that returns remapped row ID values.</li>
 * </ul>
 */
package org.opensearch.be.lucene.merge;

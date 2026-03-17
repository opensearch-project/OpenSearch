/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.IndexFilterContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lucene-specific index filter context.
 * <p>
 * Holds the Weight (per-query), and manages per-segment scorers/collectors.
 * One context per (query, reader) pair.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexFilterContext implements IndexFilterContext {

    private final Weight weight;
    private final List<LeafReaderContext> leaves;
    private final AtomicInteger nextCollectorId = new AtomicInteger(1);
    private final Map<Integer, CollectorState> collectors = new ConcurrentHashMap<>();

    public LuceneIndexFilterContext(Query query, DirectoryReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query rewritten = searcher.rewrite(query);
        this.weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        this.leaves = reader.leaves();
    }

    @Override
    public int segmentCount() {
        return leaves.size();
    }

    @Override
    public int segmentMaxDoc(int segmentOrd) {
        return leaves.get(segmentOrd).reader().maxDoc();
    }

    Weight getWeight() {
        return weight;
    }

    List<LeafReaderContext> getLeaves() {
        return leaves;
    }

    int registerCollector(DocIdSetIterator iterator, int minDoc, int maxDoc) {
        int key = nextCollectorId.getAndIncrement();
        collectors.put(key, new CollectorState(iterator, minDoc, maxDoc));
        return key;
    }

    CollectorState getCollector(int key) {
        return collectors.get(key);
    }

    void removeCollector(int key) {
        collectors.remove(key);
    }

    @Override
    public void close() {
        collectors.clear();
    }

    static class CollectorState {
        final DocIdSetIterator iterator;
        final int minDoc;
        final int maxDoc;
        int currentDoc = -1;

        CollectorState(DocIdSetIterator iterator, int minDoc, int maxDoc) {
            this.iterator = iterator;
            this.minDoc = minDoc;
            this.maxDoc = maxDoc;
        }
    }
}

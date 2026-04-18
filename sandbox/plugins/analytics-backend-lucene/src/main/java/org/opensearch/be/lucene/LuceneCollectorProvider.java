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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.IndexFilterCollectorProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lucene-backed {@link IndexFilterCollectorProvider} for Substrait-driven
 * tree query evaluation.
 * <p>
 * Manages Lucene Weights (per-query) and per-segment Scorers. Query bytes
 * are expected in UTF-8 {@code "column:value"} format, which is parsed
 * into a Lucene {@link TermQuery}.
 * <p>
 * Thread-safe: uses {@link ConcurrentHashMap} for active weights and
 * collectors, and {@link AtomicInteger} for key generation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneCollectorProvider implements IndexFilterCollectorProvider {

    private final DirectoryReader reader;
    private final ConcurrentHashMap<Integer, WeightEntry> activeWeights = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, CollectorEntry> activeCollectors = new ConcurrentHashMap<>();
    private final AtomicInteger nextProviderKey = new AtomicInteger(1);
    private final AtomicInteger nextCollectorKey = new AtomicInteger(1);

    /**
     * Tracks a Lucene Weight and its associated leaf reader contexts.
     */
    private static final class WeightEntry {
        final Weight weight;
        final List<LeafReaderContext> leaves;

        WeightEntry(Weight weight, List<LeafReaderContext> leaves) {
            this.weight = weight;
            this.leaves = leaves;
        }
    }

    /**
     * Tracks a per-segment Scorer with its doc range and owning provider key.
     */
    private static final class CollectorEntry {
        final Scorer scorer;
        final int minDoc;
        final int maxDoc;
        final int providerKey;
        int currentDoc = -1;

        CollectorEntry(Scorer scorer, int minDoc, int maxDoc, int providerKey) {
            this.scorer = scorer;
            this.minDoc = minDoc;
            this.maxDoc = maxDoc;
            this.providerKey = providerKey;
        }
    }

    /**
     * Creates a new LuceneCollectorProvider.
     *
     * @param reader the DirectoryReader for creating Weights
     */
    public LuceneCollectorProvider(DirectoryReader reader) {
        this.reader = reader;
    }

    @Override
    public int createProvider(byte[] queryBytes) throws IOException {
        String queryStr = new String(queryBytes, StandardCharsets.UTF_8);
        int colonIdx = queryStr.indexOf(':');
        if (colonIdx < 0) {
            throw new IOException("Invalid query format, expected 'column:value' but got: " + queryStr);
        }
        String field = queryStr.substring(0, colonIdx);
        String value = queryStr.substring(colonIdx + 1);

        Query query = new TermQuery(new Term(field, value));
        IndexSearcher searcher = new IndexSearcher(reader);
        Query rewritten = searcher.rewrite(query);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        int key = nextProviderKey.getAndIncrement();
        activeWeights.put(key, new WeightEntry(weight, reader.leaves()));
        return key;
    }

    @Override
    public int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
        WeightEntry entry = activeWeights.get(providerKey);
        if (entry == null) {
            return -1;
        }

        try {
            Scorer scorer = entry.weight.scorer(entry.leaves.get(segmentOrd));
            int key = nextCollectorKey.getAndIncrement();
            if (scorer == null) {
                // No matches in this segment — store null scorer, collectDocs returns empty
                activeCollectors.put(key, new CollectorEntry(null, minDoc, maxDoc, providerKey));
            } else {
                activeCollectors.put(key, new CollectorEntry(scorer, minDoc, maxDoc, providerKey));
            }
            return key;
        } catch (IOException e) {
            int key = nextCollectorKey.getAndIncrement();
            activeCollectors.put(key, new CollectorEntry(null, minDoc, maxDoc, providerKey));
            return key;
        }
    }

    @Override
    public long[] collectDocs(int collectorKey, int minDoc, int maxDoc) {
        CollectorEntry entry = activeCollectors.get(collectorKey);
        if (entry == null || entry.scorer == null) {
            return new long[0];
        }

        int effectiveMin = Math.max(minDoc, entry.minDoc);
        int effectiveMax = Math.min(maxDoc, entry.maxDoc);
        if (effectiveMin >= effectiveMax) {
            return new long[0];
        }

        BitSet bitset = new BitSet(effectiveMax - effectiveMin);
        try {
            DocIdSetIterator iterator = entry.scorer.iterator();
            int docId = entry.currentDoc;
            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId >= entry.maxDoc) {
                return new long[0];
            }
            if (docId < effectiveMin) {
                docId = iterator.advance(effectiveMin);
            }
            while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < effectiveMax) {
                bitset.set(docId - effectiveMin);
                docId = iterator.nextDoc();
            }
            entry.currentDoc = docId;
        } catch (IOException e) {
            return new long[0];
        }
        return bitset.toLongArray();
    }

    @Override
    public void releaseCollector(int collectorKey) {
        activeCollectors.remove(collectorKey);
    }

    @Override
    public void releaseProvider(int providerKey) {
        WeightEntry entry = activeWeights.remove(providerKey);
        if (entry == null) {
            return;
        }
        // Release all collectors that belong to this provider
        activeCollectors.entrySet().removeIf(e -> e.getValue().providerKey == providerKey);
    }

    @Override
    public void close() throws IOException {
        activeCollectors.clear();
        activeWeights.clear();
    }
}

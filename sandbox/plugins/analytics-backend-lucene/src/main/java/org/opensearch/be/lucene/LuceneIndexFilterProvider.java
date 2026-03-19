/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.IndexFilterProvider;

import java.io.IOException;
import java.util.BitSet;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexFilterProvider implements IndexFilterProvider<Query, LuceneIndexFilterContext> {

    @Override
    public LuceneIndexFilterContext createContext(Query query, Object reader) throws IOException {
        return new LuceneIndexFilterContext(query, (DirectoryReader) reader);
    }

    @Override
    public int createCollector(LuceneIndexFilterContext context, int segmentOrd, int minDoc, int maxDoc) {
        try {
            Scorer scorer = context.getWeight().scorer(context.getLeaves().get(segmentOrd));
            if (scorer == null) return -1;
            return context.registerCollector(scorer.iterator(), minDoc, maxDoc);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public long[] collectDocs(LuceneIndexFilterContext context, int collectorKey, int minDoc, int maxDoc) {
        LuceneIndexFilterContext.CollectorState state = context.getCollector(collectorKey);
        if (state == null) return new long[0];

        int effectiveMin = Math.max(minDoc, state.minDoc);
        int effectiveMax = Math.min(maxDoc, state.maxDoc);
        if (effectiveMin >= effectiveMax) return new long[0];

        BitSet bitset = new BitSet(effectiveMax - effectiveMin);
        try {
            DocIdSetIterator iter = state.iterator;
            int docId = state.currentDoc;
            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId >= state.maxDoc) return new long[0];
            if (docId < effectiveMin) docId = iter.advance(effectiveMin);
            while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < effectiveMax) {
                bitset.set(docId - effectiveMin);
                docId = iter.nextDoc();
            }
            state.currentDoc = docId;
        } catch (IOException e) {
            return new long[0];
        }
        return bitset.toLongArray();
    }

    @Override
    public void releaseCollector(LuceneIndexFilterContext context, int collectorKey) {
        context.removeCollector(collectorKey);
    }

    @Override
    public void close() {}
}

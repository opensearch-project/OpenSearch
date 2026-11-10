/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.stream;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;

public class SegmentIterator {
    private final FixedBitSet fixedBitSet;
    private final LeafReaderContext leaf;
    private final int segmentOrd;
    private final int batchSize;
    private final PriorityQueue<MultiSortDoc> localQueue;

    private final MappedFieldType[] sortFieldTypes;
    private final int[] multipliers;

    public SegmentIterator(FixedBitSet fixedBitSet,
                           LeafReaderContext leaf,
                           int segmentOrd,
                           int batchSize,
                           MappedFieldType[] sortFieldTypes,
                           int[] multipliers) throws IOException {
        this.fixedBitSet = fixedBitSet;
        this.leaf = leaf;
        this.segmentOrd = segmentOrd;
        this.batchSize = batchSize;
        this.sortFieldTypes = sortFieldTypes;
        this.multipliers = multipliers;
        this.localQueue = new PriorityQueue<>(batchSize) {
            @Override
            protected boolean lessThan(MultiSortDoc a, MultiSortDoc b) {
                return a.compareTo(b) < 0;
            }
        };
        addDocs();
    }

    private void addDocs() throws IOException  {
        DocIdSetIterator it = new BitSetIterator(fixedBitSet, 0);
        int docId;

        while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            int globalDocId = leaf.docBase + docId;
            Object[] sortValues = new Object[sortFieldTypes.length];
            for (int i = 0; i < sortFieldTypes.length; i++) {
                sortValues[i] = DocValueExtractor.getSortFieldValue(leaf, docId, sortFieldTypes[i]);
            }
            MultiSortDoc doc = new MultiSortDoc(globalDocId, sortValues, segmentOrd, multipliers);
            localQueue.add(doc);
        }
    }

    public MultiSortDoc next() throws IOException {
        MultiSortDoc nextDoc =  localQueue.pop();
        if (nextDoc == null) {
            addDocs();
        }
        nextDoc = localQueue.pop();
        if (nextDoc != null) {
            int localDocId = nextDoc.getDocId() - leaf.docBase;
            fixedBitSet.clear(localDocId); // ignore it in the next round
        }
        return nextDoc;
    }

    public void resetDoc(int localDocId) {
        fixedBitSet.clear(localDocId);
    }


}

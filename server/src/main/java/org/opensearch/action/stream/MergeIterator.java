/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.stream;


import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class MergeIterator {

    private final PriorityQueue<SegmentDocWrapper> globalQueue;
    private final int batchSize;
    List<SegmentIterator> segmentIterators;

    public MergeIterator(List<SegmentIterator> segmentIterators, int batchSize) throws IOException {
        this.batchSize = batchSize;
        this.globalQueue = new PriorityQueue<>(batchSize) {
            @Override
            protected boolean lessThan(SegmentDocWrapper a, SegmentDocWrapper b) {
                return a.compareTo(b) < 0;
            }
        };
        this.segmentIterators = segmentIterators;
    }

    public  List<MultiSortDoc> nextBatch() throws IOException {
        for (SegmentIterator segIter : segmentIterators) {
            MultiSortDoc topSegment = segIter.next();
            if (topSegment != null) {
                globalQueue.add(new SegmentDocWrapper(topSegment, segIter));
            }
        }

        List<MultiSortDoc> batch = new ArrayList<>(batchSize);
        for (int count = 0; count < batchSize; count++) {
            if (globalQueue.size() == 0) {
                break;
            }
            SegmentDocWrapper top = globalQueue.pop();
            batch.add(top.doc);
            MultiSortDoc nextDoc = top.segmentIterator.next();
            if (nextDoc != null) {
                globalQueue.add(new SegmentDocWrapper(nextDoc, top.segmentIterator));
            }
        }
        return batch;
    }


    public boolean hasNext() {
        return globalQueue.size() > 0;
    }

    private static final class SegmentDocWrapper implements Comparable<SegmentDocWrapper> {
        final MultiSortDoc doc;
        final SegmentIterator segmentIterator;

        SegmentDocWrapper(MultiSortDoc doc, SegmentIterator segmentIterator) {
            this.doc = doc;
            this.segmentIterator = segmentIterator;
        }

        @Override
        public int compareTo(SegmentDocWrapper other) {
            return this.doc.compareTo(other.doc);
        }
    }
}

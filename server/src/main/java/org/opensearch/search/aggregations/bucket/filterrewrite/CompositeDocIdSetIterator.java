/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * A composite iterator over multiple DocIdSetIterators where each document
 * belongs to exactly one bucket within a single segment.
 */
public class CompositeDocIdSetIterator extends DocIdSetIterator {
    private final DocIdSetIterator[] iterators;

    // Track active iterators to avoid scanning all
    private final int[] activeIterators;  // non-exhausted iterators to its bucket
    private int numActiveIterators;       // Number of non-exhausted iterators

    private int currentDoc = -1;
    private int currentBucket = -1;

    public CompositeDocIdSetIterator(DocIdSetIterator[] iterators) {
        this.iterators = iterators;
        int numBuckets = iterators.length;
        this.activeIterators = new int[numBuckets];
        this.numActiveIterators = 0;

        // Initialize active iterator tracking
        for (int i = 0; i < numBuckets; i++) {
            if (iterators[i] != null) {
                activeIterators[numActiveIterators++] = i;
            }
        }
    }

    @Override
    public int docID() {
        return currentDoc;
    }

    public int getCurrentBucket() {
        return currentBucket;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(currentDoc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        if (target == NO_MORE_DOCS || numActiveIterators == 0) {
            currentDoc = NO_MORE_DOCS;
            currentBucket = -1;
            return NO_MORE_DOCS;
        }

        int minDoc = NO_MORE_DOCS;
        int minBucket = -1;
        int remainingActive = 0;  // Counter for non-exhausted iterators

        // Only check currently active iterators
        for (int i = 0; i < numActiveIterators; i++) {
            int bucket = activeIterators[i];
            DocIdSetIterator iterator = iterators[bucket];

            int doc = iterator.docID();
            if (doc < target) {
                doc = iterator.advance(target);
            }

            if (doc == NO_MORE_DOCS) {
                // Iterator is exhausted, don't include it in active set
                continue;
            }

            // Keep this iterator in our active set
            activeIterators[remainingActive] = bucket;
            remainingActive++;

            if (doc < minDoc) {
                minDoc = doc;
                minBucket = bucket;
            }
        }

        // Update count of active iterators
        numActiveIterators = remainingActive;

        currentDoc = minDoc;
        currentBucket = minBucket;

        return currentDoc;
    }

    @Override
    public long cost() {
        long cost = 0;
        for (int i = 0; i < numActiveIterators; i++) {
            DocIdSetIterator iterator = iterators[activeIterators[i]];
            cost += iterator.cost();
        }
        return cost;
    }
}

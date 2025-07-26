/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;

import java.io.IOException;

/**
 * A resumable DocIdSetIterator that can be used to score documents in batches.
 * This class wraps a ScorerSupplier and creates a new Scorer/DocIdSetIterator only when needed.
 * It maintains state between calls to enable resuming from where it left off.
 *
 * This implementation is specifically designed for the approximation framework to enable
 * early termination while preserving state between scoring cycles.
 */
public class ResumableDISI extends DocIdSetIterator {
    private static final int DEFAULT_BATCH_SIZE = 10_000;

    private final ScorerSupplier scorerSupplier;
    private DocIdSetIterator currentDisi;
    private final int batchSize;
    private boolean exhausted = false;

    // State tracking - track batches, not individual document movements
    private int lastDocID = -1;
    private int batchCount = 0;  // How many batches we've created
    private boolean needsNewBatch = true;  // Whether we need to create a new batch

    /**
     * Creates a new ResumableDISI with the default batch size of 10,000 documents.
     *
     * @param scorerSupplier The scorer supplier to get scorers from
     */
    public ResumableDISI(ScorerSupplier scorerSupplier) {
        this(scorerSupplier, DEFAULT_BATCH_SIZE);
    }

    /**
     * Creates a new ResumableDISI with the specified batch size.
     *
     * @param scorerSupplier The scorer supplier to get scorers from
     * @param batchSize The number of documents to score in each batch
     */
    public ResumableDISI(ScorerSupplier scorerSupplier, int batchSize) {
        this.scorerSupplier = scorerSupplier;
        this.batchSize = batchSize;
    }

    /**
     * Initializes or resets the internal DocIdSetIterator.
     * For approximatable queries, this leverages their existing resumable mechanism.
     * For non-approximatable queries, this creates new scorers as needed.
     *
     * @return The current DocIdSetIterator
     * @throws IOException If there's an error getting the scorer
     */
    private DocIdSetIterator getOrCreateDisi() throws IOException {
        if (exhausted) {
            return currentDisi; // Already exhausted, no need to create a new one
        }

        if (currentDisi == null || needsNewBatch) {
            // Get a new scorer and its iterator
            // For approximatable queries, the scorer supplier will handle resumable state internally
            Scorer scorer = scorerSupplier.get(scorerSupplier.cost());
            currentDisi = scorer.iterator();

            // For non-approximatable queries, we need to advance past the last document
            // For approximatable queries, they handle this internally via their BKD state
            if (lastDocID >= 0) {
                // Check if we need to advance (for non-approximatable queries)
                if (currentDisi.docID() <= lastDocID) {
                    currentDisi.advance(lastDocID + 1);
                }
            }

            // Mark that we've created a new batch
            batchCount++;
            needsNewBatch = false;
        }

        return currentDisi;
    }

    @Override
    public int docID() {
        if (currentDisi == null) {
            return -1;
        }
        return currentDisi.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        DocIdSetIterator disi = getOrCreateDisi();
        int doc = disi.nextDoc();

        if (doc != NO_MORE_DOCS) {
            lastDocID = doc;
        } else {
            exhausted = true;
        }

        return doc;
    }

    @Override
    public int advance(int target) throws IOException {
        DocIdSetIterator disi = getOrCreateDisi();
        int doc = disi.advance(target);

        if (doc != NO_MORE_DOCS) {
            lastDocID = doc;
        } else {
            exhausted = true;
        }

        return doc;
    }

    @Override
    public long cost() {
        return scorerSupplier.cost();
    }

    /**
     * Resets the iterator to start a new batch from the last document ID.
     * This allows the caller to continue scoring from where it left off.
     */
    public void resetForNextBatch() {
        if (!exhausted) {
            needsNewBatch = true; // Mark that we need a new batch on next access
        }
    }

    /**
     * Returns the number of batches created so far.
     *
     * @return The number of batches created
     */
    public int getBatchCount() {
        return batchCount;
    }

    /**
     * Returns whether this iterator has been exhausted.
     *
     * @return true if there are no more documents to score
     */
    public boolean isExhausted() {
        return exhausted;
    }

    /**
     * Returns the last document ID that was scored.
     *
     * @return The last document ID, or -1 if no documents have been scored
     */
    public int getLastDocID() {
        return lastDocID;
    }

    /**
     * Class to track the state of BKD tree traversal.
     */
    public static class BKDState {
        private PointValues.PointTree currentTree;
        private boolean isExhausted = false;
        private long docCount = 0;
        private boolean inProgress = false;

        public PointValues.PointTree getCurrentTree() {
            return currentTree;
        }

        public void setCurrentTree(PointValues.PointTree tree) {
            if (tree != null) {
                this.currentTree = tree.clone();
            } else {
                this.currentTree = null;
            }
        }

        public boolean isExhausted() {
            return isExhausted;
        }

        public void setExhausted(boolean exhausted) {
            this.isExhausted = exhausted;
        }

        public long getDocCount() {
            return docCount;
        }

        public void setDocCount(long count) {
            this.docCount = count;
        }

        public boolean isInProgress() {
            return inProgress;
        }

        public void setInProgress(boolean inProgress) {
            this.inProgress = inProgress;
        }
    }
}

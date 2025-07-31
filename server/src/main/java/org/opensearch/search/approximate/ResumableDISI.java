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
 * A resumable DocIdSetIterator that internally expands when it reaches NO_MORE_DOCS.
 * On the surface, this behaves identically to a regular DISI, but internally it can
 * expand by scoring additional documents when needed.
 *
 * The expansion is completely internal - external callers see a normal DISI interface
 * that continues to return documents even after initially hitting NO_MORE_DOCS.
 */
public class ResumableDISI extends DocIdSetIterator {
    private static final int DEFAULT_EXPANSION_SIZE = 10_000;

    private final ScorerSupplier scorerSupplier;
    private final int expansionSize;

    // Current state
    private DocIdSetIterator currentDisi;
    private int currentDocId = -1;
    private boolean fullyExhausted = false;

    private int documentsScored = 0; // Total documents scored across all expansions

    /**
     * Creates a new ResumableDISI with the default expansion size of 10,000 documents.
     *
     * @param scorerSupplier The scorer supplier to get scorers from
     */
    public ResumableDISI(ScorerSupplier scorerSupplier) {
        this(scorerSupplier, DEFAULT_EXPANSION_SIZE);
    }

    /**
     * Creates a new ResumableDISI with the specified expansion size.
     *
     * @param scorerSupplier The scorer supplier to get scorers from
     * @param expansionSize The number of documents to score in each expansion
     */
    public ResumableDISI(ScorerSupplier scorerSupplier, int expansionSize) {
        this.scorerSupplier = scorerSupplier;
        this.expansionSize = expansionSize;
    }

    @Override
    public int docID() {
        return currentDocId;
    }

    @Override
    public int nextDoc() throws IOException {
        if (fullyExhausted) {
            return NO_MORE_DOCS;
        }

        // If we don't have a current iterator, get one
        if (currentDisi == null) {
            if (!expandInternally()) {
                return NO_MORE_DOCS;
            }
            // expandInternally() already positioned us on the first document
            return currentDocId;
        }

        // Try to get the next document from current iterator
        int doc = currentDisi.nextDoc();

        if (doc != NO_MORE_DOCS) {
            currentDocId = doc;
            return doc;
        }

        // Current iterator exhausted, try to expand internally
        if (expandInternally()) {
            // expandInternally() already positioned us on the first document of the new batch
            return currentDocId;
        }

        // No more expansion possible
        currentDocId = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
        if (fullyExhausted) {
            return NO_MORE_DOCS;
        }

        // If we don't have a current iterator, get one
        if (currentDisi == null) {
            if (!expandInternally()) {
                return NO_MORE_DOCS;
            }
            // If the first document is >= target, we're good
            if (currentDocId >= target) {
                return currentDocId;
            }
            // Otherwise, advance to target
            int doc = currentDisi.advance(target);
            if (doc != NO_MORE_DOCS) {
                currentDocId = doc;
                return doc;
            }
            // Fall through to try expansion
        } else {
            // Try to advance current iterator
            int doc = currentDisi.advance(target);
            if (doc != NO_MORE_DOCS) {
                currentDocId = doc;
                return doc;
            }
            // Current iterator exhausted, try to expand
        }

        // Current iterator exhausted, try to expand internally
        if (expandInternally()) {
            // If the first document of new batch is >= target, we're good
            if (currentDocId >= target) {
                return currentDocId;
            }
            // Otherwise, advance to target
            int doc = currentDisi.advance(target);
            if (doc != NO_MORE_DOCS) {
                currentDocId = doc;
                return doc;
            }
        }

        // No more expansion possible
        currentDocId = NO_MORE_DOCS;
        fullyExhausted = true;
        return NO_MORE_DOCS;
    }

    /**
     * Expands the iterator internally by getting a new scorer from the supplier.
     * This is called when we hit NO_MORE_DOCS but more documents might be available.
     *
     * @return true if expansion was successful, false if fully exhausted
     * @throws IOException If there's an error getting the scorer
     */
    private boolean expandInternally() throws IOException {
        if (fullyExhausted) {
            return false;
        }

        // Get a new scorer from the supplier - this will resume from saved BKD state
        Scorer scorer = scorerSupplier.get(scorerSupplier.cost());
        if (scorer == null) {
            fullyExhausted = true;
            return false;
        }

        currentDisi = scorer.iterator();
        documentsScored += expansionSize; // Track total documents scored

        // Check if the new iterator has any documents
        int firstDoc = currentDisi.nextDoc();
        if (firstDoc == NO_MORE_DOCS) {
            fullyExhausted = true;
            return false;
        }

        // Position the iterator on the first document
        currentDocId = firstDoc;
        return true;
    }

    @Override
    public long cost() {
        return scorerSupplier.cost();
    }

    /**
     * Returns whether this iterator has been fully exhausted.
     *
     * @return true if there are no more documents to score
     */
    public boolean isExhausted() {
        return fullyExhausted;
    }

    /**
     * Returns the total number of documents scored across all expansions.
     *
     * @return The total number of documents scored
     */
    public int getDocumentsScored() {
        return documentsScored;
    }

    /**
     * Class to track the state of BKD tree traversal.
     */
    public static class BKDState {
        private PointValues.PointTree currentTree;
        private boolean isExhausted = false;
        private long docCount = 0;
        private boolean inProgress = false;
        private boolean hasSetTree = false;

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

        public boolean hasSetTree() {
            return hasSetTree;
        }

        public void setHasSetTree(boolean hasSet) {
            this.hasSetTree = hasSetTree;
        }

    }
}

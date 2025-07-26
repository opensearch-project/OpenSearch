/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.List;

/**
 * A custom conjunction coordinator that understands both resumable and regular iterators.
 * This class coordinates multiple DocIdSetIterators (which may include ResumableDISIs) to find documents that match all clauses.
 */
public class ApproximateConjunctionDISI extends DocIdSetIterator {
    private final List<DocIdSetIterator> iterators;
    private final DocIdSetIterator lead;
    private final DocIdSetIterator[] others;
    private int doc = -1;
    private int lastValidDoc = -1; // Track the last valid document before NO_MORE_DOCS
    private boolean exhausted = false;

    /**
     * Creates a new ApproximateConjunctionDISI.
     *
     * @param iterators The iterators to coordinate (mix of ResumableDISI and regular DocIdSetIterator)
     */
    public ApproximateConjunctionDISI(List<DocIdSetIterator> iterators) {
        if (iterators.isEmpty()) {
            throw new IllegalArgumentException("No iterators provided");
        }

        this.iterators = iterators;

        // Sort iterators by cost (ascending)
        iterators.sort((a, b) -> Long.compare(a.cost(), b.cost()));

        // Use the cheapest iterator as the lead
        this.lead = iterators.get(0);

        // Store the other iterators
        this.others = new DocIdSetIterator[iterators.size() - 1];
        for (int i = 1; i < iterators.size(); i++) {
            others[i - 1] = iterators.get(i);
        }
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        if (exhausted) {
            return doc = NO_MORE_DOCS;
        }

        // Advance the lead iterator
        doc = lead.nextDoc();

        if (doc == NO_MORE_DOCS) {
            // Before marking as exhausted, check if any ResumableDISI can be expanded
            if (canExpandAnyResumableDISI()) {
                // Don't mark as exhausted yet - caller can expand and try again
                return doc;
            }
            exhausted = true;
            return doc;
        }

        // Try to align all other iterators
        return doNext(doc);
    }

    @Override
    public int advance(int target) throws IOException {
        if (exhausted) {
            return doc = NO_MORE_DOCS;
        }

        // Advance the lead iterator
        doc = lead.advance(target);

        if (doc == NO_MORE_DOCS) {
            // Before marking as exhausted, check if any ResumableDISI can be expanded
            if (canExpandAnyResumableDISI()) {
                // Don't mark as exhausted yet - caller can expand and try again
                return doc;
            }
            exhausted = true;
            return doc;
        }

        // Try to align all other iterators
        return doNext(doc);
    }

    /**
     * Check if any ResumableDISI in the iterators can be expanded (not exhausted)
     */
    private boolean canExpandAnyResumableDISI() {
        for (DocIdSetIterator iterator : iterators) {
            if (iterator instanceof ResumableDISI) {
                ResumableDISI resumableDISI = (ResumableDISI) iterator;
                if (!resumableDISI.isExhausted()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Reset the exhausted state so the conjunction can continue after ResumableDISIs are expanded.
     * Preserves the current document position to avoid reprocessing documents.
     */
    public void resetAfterExpansion() {
        // Only reset if we're not truly exhausted (i.e., some ResumableDISI was expanded)
        if (canExpandAnyResumableDISI()) {
            exhausted = false;
            // Set doc to the last valid document we processed
            // The next call to nextDoc() will advance from this position
            doc = lastValidDoc;
        }
    }

    /**
     * Coordinates multiple iterators to find documents that match all clauses.
     * This is similar to ConjunctionDISI.doNext() but adapted for mixed iterator types.
     */
    private int doNext(int doc) throws IOException {
        advanceHead: for (;;) {
            // Try to align all other iterators with the lead
            for (DocIdSetIterator other : others) {
                if (other.docID() < doc) {
                    final int next = other.advance(doc);
                    if (next > doc) {
                        // This iterator is ahead, advance the lead to catch up
                        doc = lead.advance(next);
                        if (doc == NO_MORE_DOCS) {
                            // Before marking as exhausted, check if any ResumableDISI can be expanded
                            if (canExpandAnyResumableDISI()) {
                                // Don't mark as exhausted yet - caller can expand and try again
                                return this.doc = NO_MORE_DOCS;
                            }
                            exhausted = true;
                            return this.doc = NO_MORE_DOCS;
                        }
                        continue advanceHead;
                    }
                }
            }

            // All iterators are aligned at the current doc
            lastValidDoc = doc; // Remember this valid document
            return this.doc = doc;
        }
    }

    @Override
    public long cost() {
        // Return the cost of the cheapest iterator
        return lead.cost();
    }

    /**
     * Returns whether this iterator has been exhausted.
     *
     * @return true if there are no more documents to score
     */
    public boolean isExhausted() {
        return exhausted;
    }
}

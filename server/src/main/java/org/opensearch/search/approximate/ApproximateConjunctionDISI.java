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
 * A conjunction of DocIdSetIterators with support for ResumableDISI expansion.
 * Closely mirrors Lucene's ConjunctionDISI architecture with lead1, lead2, and others.
 */
public class ApproximateConjunctionDISI extends DocIdSetIterator {

    final DocIdSetIterator lead1, lead2;
    final DocIdSetIterator[] others;

    private final List<DocIdSetIterator> allIterators;

    public ApproximateConjunctionDISI(List<DocIdSetIterator> iterators) {
        if (iterators.size() < 2) {
            throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
        }

        this.allIterators = iterators;

        // Follow Lucene's exact structure
        this.lead1 = iterators.get(0);
        this.lead2 = iterators.get(1);
        this.others = iterators.subList(2, iterators.size()).toArray(new DocIdSetIterator[0]);
    }

    @Override
    public int docID() {
        return lead1.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return doNext(lead1.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
        return doNext(lead1.advance(target));
    }

    /**
     * Core conjunction logic adapted from Lucene's ConjunctionDISI.doNext()
     * with resumable expansion support.
     */
    private int doNext(int doc) throws IOException {
        advanceHead:
        for (; ; ) {
            // Handle NO_MORE_DOCS with resumable expansion
            if (doc == NO_MORE_DOCS) {
                if (tryExpandResumableDISIs()) {
                    // After expansion, get the next document from lead1
                    doc = lead1.nextDoc();
                    if (doc == NO_MORE_DOCS) {
                        return NO_MORE_DOCS; // Truly exhausted
                    }
                    // Continue with the new document
                } else {
                    return NO_MORE_DOCS; // No expansion possible
                }
            }

            // Find agreement between the two iterators with the lower costs
            // We special case them because they do not need the
            // 'other.docID() < doc' check that the 'others' iterators need
            final int next2 = lead2.advance(doc);
            if (next2 != doc) {
                doc = lead1.advance(next2);
                if (next2 != doc) {
                    continue;
                }
            }

            // Then find agreement with other iterators
            for (DocIdSetIterator other : others) {
                // other.docID() may already be equal to doc if we "continued advanceHead"
                // on the previous iteration and the advance on the lead exactly matched.
                if (other.docID() < doc) {
                    final int next = other.advance(doc);

                    if (next > doc) {
                        // iterator beyond the current doc - advance lead and continue to the new highest doc.
                        doc = lead1.advance(next);
                        continue advanceHead;
                    }
                }
            }

            // Success - all iterators are on the same doc
            return doc;
        }
    }

    /**
     * Try to expand ResumableDISIs when we hit NO_MORE_DOCS
     * @return true if any ResumableDISI was expanded
     */
    private boolean tryExpandResumableDISIs() throws IOException {
        boolean anyExpanded = false;

        // Check all iterators for expansion
        for (DocIdSetIterator iterator : allIterators) {
            if (iterator instanceof ResumableDISI) {
                ResumableDISI resumable = (ResumableDISI) iterator;
                if (!resumable.isExhausted()) {
                    resumable.resetForNextBatch();
                    anyExpanded = true;
                }
            }
        }

        return anyExpanded;
    }

    @Override
    public long cost() {
        long minCost = Long.MAX_VALUE;
        for (DocIdSetIterator iterator : allIterators) {
            minCost = Math.min(minCost, iterator.cost());
        }
        return minCost;
    }

    /**
     * Reset method for compatibility (no longer needed with new architecture)
     */
    public void resetAfterExpansion() throws IOException {
        // No-op - expansion is now handled directly in doNext()
    }
}

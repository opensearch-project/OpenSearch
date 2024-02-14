/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Clone of {@link org.apache.lucene.search} {@code DisjunctionScorer.java} in lucene with following modifications -
 * 1. {@link #removeAllDISIsOnCurrentDoc()} - it removes all the DISIs for subscorer pointing to current doc. This is
 * helpful in dynamic pruning for Cardinality aggregation, where once a term is found, it becomes irrelevant for
 * rest of the search space, so this term's subscorer DISI can be safely removed from list of subscorer to process.
 * <p>
 * 2. {@link #removeAllDISIsOnCurrentDoc()} breaks the invariant of Conjuction DISI i.e. the docIDs of all sub-scorers should be
 * less than or equal to current docID iterator is pointing to. When we remove elements from priority, it results in heapify action, which modifies
 * the top of the priority queye, which represents the current docID for subscorers here. To address this, we are wrapping the
 * iterator with {@link SlowDocIdPropagatorDISI} which keeps the iterator pointing to last docID before {@link #removeAllDISIsOnCurrentDoc()}
 * is called and updates this docID only when next() or advance() is called.
 */
public class DisjunctionWithDynamicPruningScorer extends Scorer {

    private final boolean needsScores;
    private final DisiPriorityQueue subScorers;
    private final DocIdSetIterator approximation;
    private final TwoPhase twoPhase;

    private Integer docID;

    public DisjunctionWithDynamicPruningScorer(Weight weight, List<Scorer> subScorers)
        throws IOException {
        super(weight);
        if (subScorers.size() <= 1) {
            throw new IllegalArgumentException("There must be at least 2 subScorers");
        }
        this.subScorers = new DisiPriorityQueue(subScorers.size());
        for (Scorer scorer : subScorers) {
            final DisiWrapper w = new DisiWrapper(scorer);
            this.subScorers.add(w);
        }
        this.needsScores = false;
        this.approximation = new DisjunctionDISIApproximation(this.subScorers);

        boolean hasApproximation = false;
        float sumMatchCost = 0;
        long sumApproxCost = 0;
        // Compute matchCost as the average over the matchCost of the subScorers.
        // This is weighted by the cost, which is an expected number of matching documents.
        for (DisiWrapper w : this.subScorers) {
            long costWeight = (w.cost <= 1) ? 1 : w.cost;
            sumApproxCost += costWeight;
            if (w.twoPhaseView != null) {
                hasApproximation = true;
                sumMatchCost += w.matchCost * costWeight;
            }
        }

        if (hasApproximation == false) { // no sub scorer supports approximations
            twoPhase = null;
        } else {
            final float matchCost = sumMatchCost / sumApproxCost;
            twoPhase = new TwoPhase(approximation, matchCost);
        }
    }

    public void removeAllDISIsOnCurrentDoc() {
        docID = this.docID();
        while (subScorers.size() > 0 && subScorers.top().doc == docID) {
            subScorers.pop();
        }
    }

    @Override
    public DocIdSetIterator iterator() {
       DocIdSetIterator disi = getIterator();
       docID = disi.docID();
       return new SlowDocIdPropagatorDISI(getIterator(), docID);
    }

    private static class SlowDocIdPropagatorDISI extends DocIdSetIterator {
        DocIdSetIterator disi;

        Integer curDocId;

        SlowDocIdPropagatorDISI(DocIdSetIterator disi, Integer curDocId) {
            this.disi = disi;
            this.curDocId = curDocId;
        }

        @Override
        public int docID() {
            assert curDocId <= disi.docID();
            return curDocId;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(curDocId + 1);
        }

        @Override
        public int advance(int i) throws IOException {
            if (i <= disi.docID()) {
                // since we are slow propagating docIDs, it may happen the disi is already advanced to a higher docID than i
                // in such scenarios we can simply return the docID where disi is pointing to and update the curDocId
                curDocId = disi.docID();
                return disi.docID();
            }
            curDocId = disi.advance(i);
            return curDocId;
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }

    private DocIdSetIterator getIterator() {
        if (twoPhase != null) {
            return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
        } else {
            return approximation;
        }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        return twoPhase;
    }

    @Override
    public float getMaxScore(int i) throws IOException {
        return 0;
    }

    private class TwoPhase extends TwoPhaseIterator {

        private final float matchCost;
        // list of verified matches on the current doc
        DisiWrapper verifiedMatches;
        // priority queue of approximations on the current doc that have not been verified yet
        final PriorityQueue<DisiWrapper> unverifiedMatches;

        private TwoPhase(DocIdSetIterator approximation, float matchCost) {
            super(approximation);
            this.matchCost = matchCost;
            unverifiedMatches =
                new PriorityQueue<DisiWrapper>(DisjunctionWithDynamicPruningScorer.this.subScorers.size()) {
                    @Override
                    protected boolean lessThan(DisiWrapper a, DisiWrapper b) {
                        return a.matchCost < b.matchCost;
                    }
                };
        }

        DisiWrapper getSubMatches() throws IOException {
            // iteration order does not matter
            for (DisiWrapper w : unverifiedMatches) {
                if (w.twoPhaseView.matches()) {
                    w.next = verifiedMatches;
                    verifiedMatches = w;
                }
            }
            unverifiedMatches.clear();
            return verifiedMatches;
        }

        @Override
        public boolean matches() throws IOException {
            verifiedMatches = null;
            unverifiedMatches.clear();

            for (DisiWrapper w = subScorers.topList(); w != null; ) {
                DisiWrapper next = w.next;

                if (w.twoPhaseView == null) {
                    // implicitly verified, move it to verifiedMatches
                    w.next = verifiedMatches;
                    verifiedMatches = w;

                    if (needsScores == false) {
                        // we can stop here
                        return true;
                    }
                } else {
                    unverifiedMatches.add(w);
                }
                w = next;
            }

            if (verifiedMatches != null) {
                return true;
            }

            // verify subs that have an two-phase iterator
            // least-costly ones first
            while (unverifiedMatches.size() > 0) {
                DisiWrapper w = unverifiedMatches.pop();
                if (w.twoPhaseView.matches()) {
                    w.next = null;
                    verifiedMatches = w;
                    return true;
                }
            }

            return false;
        }

        @Override
        public float matchCost() {
            return matchCost;
        }
    }


    @Override
    public final int docID() {
        return subScorers.top().doc;
    }

    DisiWrapper getSubMatches() throws IOException {
        if (twoPhase == null) {
            return subScorers.topList();
        } else {
            return twoPhase.getSubMatches();
        }
    }

    @Override
    public final float score() throws IOException {
        return score(getSubMatches());
    }

    protected float score(DisiWrapper topList) throws IOException {
        return 1f;
    }

    @Override
    public final Collection<ChildScorable> getChildren() throws IOException {
        ArrayList<ChildScorable> children = new ArrayList<>();
        for (DisiWrapper scorer = getSubMatches(); scorer != null; scorer = scorer.next) {
            children.add(new ChildScorable(scorer.scorer, "SHOULD"));
        }
        return children;
    }
}

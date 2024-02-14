/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.search.aggregations.support.FieldContext;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DynamicPruningCollectorWrapper extends CardinalityAggregator.Collector {

    private final LeafReaderContext ctx;
    private final DisjunctionWithDynamicPruningScorer disjunctionScorer;
    private final DocIdSetIterator disi;
    private final CardinalityAggregator.Collector delegateCollector;

    DynamicPruningCollectorWrapper(CardinalityAggregator.Collector delegateCollector,
                                   SearchContext context, LeafReaderContext ctx, FieldContext fieldContext,
                                   ValuesSource.Bytes.WithOrdinals source) throws IOException {
        this.ctx = ctx;
        this.delegateCollector = delegateCollector;
        final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
        boolean isCardinalityLow = ordinalValues.getValueCount() < 10;
        boolean isCardinalityAggregationOnlyAggregation = true;
        boolean isFieldSupportedForDynamicPruning = true;
        if (isCardinalityLow && isCardinalityAggregationOnlyAggregation && isFieldSupportedForDynamicPruning) {
            // create disjunctions from terms
            // this logic should be pluggable depending on the type of leaf bucket collector by CardinalityAggregator
            TermsEnum terms = ordinalValues.termsEnum();
            Weight weight = context.searcher().createWeight(context.searcher().rewrite(context.query()), ScoreMode.COMPLETE_NO_SCORES, 1f);
            Map<Long, Boolean> found = new HashMap<>();
            List<Scorer> subScorers = new ArrayList<>();
            while (terms.next() != null && !found.containsKey(terms.ord())) {
                // TODO can we get rid of terms previously encountered in other segments?
                TermQuery termQuery = new TermQuery(new Term(fieldContext.field(), terms.term()));
                Weight subWeight = context.searcher().createWeight(termQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
                Scorer scorer = subWeight.scorer(ctx);
                if (scorer != null) {
                    subScorers.add(scorer);
                }
                found.put(terms.ord(), true);
            }
            disjunctionScorer = new DisjunctionWithDynamicPruningScorer(weight, subScorers);
            disi = ConjunctionUtils.intersectScorers(List.of(disjunctionScorer, weight.scorer(ctx)));
        } else {
            disjunctionScorer = null;
            disi = null;
        }
    }

    @Override
    public void collect(int doc, long bucketOrd) throws IOException {
        if (disi == null || disjunctionScorer == null) {
            delegateCollector.collect(doc, bucketOrd);
        } else {
            // perform the full iteration using dynamic pruning of DISIs and return right away
            disi.advance(doc);
            int currDoc = disi.docID();
            assert currDoc == doc;
            final Bits liveDocs = ctx.reader().getLiveDocs();
            assert liveDocs == null || liveDocs.get(currDoc);
            do {
                if (liveDocs == null || liveDocs.get(currDoc)) {
                    delegateCollector.collect(currDoc, bucketOrd);
                    disjunctionScorer.removeAllDISIsOnCurrentDoc();
                }
                currDoc = disi.nextDoc();
            } while (currDoc != DocIdSetIterator.NO_MORE_DOCS);
            throw new CollectionTerminatedException();
        }
    }

    @Override
    public void close() {
        delegateCollector.close();
    }

    @Override
    public void postCollect() throws IOException {
        delegateCollector.postCollect();
    }
}

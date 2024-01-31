package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class MatchedQueriesPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) throws IOException {
        Map<String, Query> namedQueries = collectNamedQueries(context);
        if (namedQueries.isEmpty()) {
            return null;
        }

        Map<String, Weight> weights = prepareWeights(context, namedQueries);

        return context.includeNamedQueriesScore() ? createScoringProcessor(weights) : createNonScoringProcessor(weights);
    }

    private Map<String, Query> collectNamedQueries(FetchContext context) {
        Map<String, Query> namedQueries = new HashMap<>();
        Optional.ofNullable(context.parsedQuery()).ifPresent(parsedQuery -> namedQueries.putAll(parsedQuery.namedFilters()));
        Optional.ofNullable(context.parsedPostFilter()).ifPresent(parsedPostFilter -> namedQueries.putAll(parsedPostFilter.namedFilters()));
        return namedQueries;
    }

    private Map<String, Weight> prepareWeights(FetchContext context, Map<String, Query> namedQueries) throws IOException {
        Map<String, Weight> weights = new HashMap<>();
        for (Map.Entry<String, Query> entry : namedQueries.entrySet()) {
            ScoreMode scoreMode = context.includeNamedQueriesScore() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
            weights.put(entry.getKey(), context.searcher().createWeight(context.searcher().rewrite(entry.getValue()), scoreMode, 1));
        }
        return weights;
    }

    private FetchSubPhaseProcessor createScoringProcessor(Map<String, Weight> weights) {
        return new FetchSubPhaseProcessor() {
            final Map<String, Scorer> matchingScorers = new HashMap<>();

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                setupScorers(readerContext, weights, matchingScorers);
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, Float> matches = new LinkedHashMap<>();
                int docId = hitContext.docId();
                for (Map.Entry<String, Scorer> entry : matchingScorers.entrySet()) {
                    Scorer scorer = entry.getValue();
                    if (scorer.iterator().docID() < docId) {
                        scorer.iterator().advance(docId);
                    }
                    if (scorer.iterator().docID() == docId) {
                        matches.put(entry.getKey(), scorer.score());
                    }
                }
                hitContext.hit().matchedQueriesWithScores(matches);
            }
        };
    }

    private FetchSubPhaseProcessor createNonScoringProcessor(Map<String, Weight> weights) {
        return new FetchSubPhaseProcessor() {
            final Map<String, Bits> matchingBits = new HashMap<>();

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                setupMatchingBits(readerContext, weights, matchingBits);
            }

            @Override
            public void process(HitContext hitContext) {
                List<String> matches = new ArrayList<>();
                int docId = hitContext.docId();
                for (Map.Entry<String, Bits> entry : matchingBits.entrySet()) {
                    if (entry.getValue().get(docId)) {
                        matches.add(entry.getKey());
                    }
                }
                hitContext.hit().matchedQueries(matches.toArray(new String[0]));
            }
        };
    }

    private void setupScorers(LeafReaderContext readerContext, Map<String, Weight> weights, Map<String, Scorer> scorers)
        throws IOException {
        scorers.clear();
        for (Map.Entry<String, Weight> entry : weights.entrySet()) {
            ScorerSupplier scorerSupplier = entry.getValue().scorerSupplier(readerContext);
            if (scorerSupplier != null) {
                Scorer scorer = scorerSupplier.get(0L);
                if (scorer != null) {
                    scorers.put(entry.getKey(), scorer);
                }
            }
        }
    }

    private void setupMatchingBits(LeafReaderContext readerContext, Map<String, Weight> weights, Map<String, Bits> bitsMap)
        throws IOException {
        bitsMap.clear();
        for (Map.Entry<String, Weight> entry : weights.entrySet()) {
            ScorerSupplier scorerSupplier = entry.getValue().scorerSupplier(readerContext);
            if (scorerSupplier != null) {
                Bits bits = Lucene.asSequentialAccessBits(readerContext.reader().maxDoc(), scorerSupplier);
                bitsMap.put(entry.getKey(), bits);
            }
        }
    }
}

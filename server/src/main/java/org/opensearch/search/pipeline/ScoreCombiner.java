/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstracts combination of scores in query search results.
 */
public class ScoreCombiner {
    private static final Logger logger = LogManager.getLogger(ScoreCombiner.class);

    public static final Float MAX_SCORE_WHEN_NO_HITS_FOUND = 0.0f;
    // Tie-breaker to merge multiple top docs
    private static final Comparator<ScoreDoc> SORTING_TIE_BREAKER = (o1, o2) -> {
        int scoreComparison = Double.compare(o1.score, o2.score);
        if (scoreComparison != 0) {
            return scoreComparison;
        }

        int docIdComparison = Integer.compare(o1.doc, o2.doc);
        if (docIdComparison != 0) {
            return docIdComparison;
        }

        // When duplicate result found then both score and doc ID are equal (o1.score == o2.score && o1.doc == o2.doc) then return 1
        return 1;
    };

    /**
     * Performs score combination based on input combination technique. Mutates input object by updating combined scores
     * Main steps we're doing for combination:
     * - create map of normalized scores per doc id
     * - using normalized scores create another map of combined scores per doc id
     * - count max number of hits among sub-queries
     * - sort documents by scores and take first "max number" of docs
     * - update query search results with normalized scores
     * Different score combination techniques are different in step 2, where we create map of "doc id" - "combined score",
     * other steps are same for all techniques.
     *
     * @param combineScoresDTO   contains details of query top docs, score combination technique and sort is enabled or disabled.
     */
    public void combineScores(final CombineScoresDto combineScoresDTO) {
        // iterate over results from each shard. Every CompoundTopDocs object has results from
        // multiple sub queries, doc ids may repeat for each sub query results
        ScoreCombinationTechnique scoreCombinationTechnique = combineScoresDTO.getScoreCombinationTechnique();
        Sort sort = combineScoresDTO.getSort();
        combineScoresDTO.getQueryTopDocs()
            .forEach(compoundQueryTopDocs -> combineShardScores(scoreCombinationTechnique, compoundQueryTopDocs, sort));
    }

    private void combineShardScores(
        final ScoreCombinationTechnique scoreCombinationTechnique,
        final CompoundTopDocs compoundQueryTopDocs,
        final Sort sort
    ) {
        if (Objects.isNull(compoundQueryTopDocs) || compoundQueryTopDocs.getTotalHits().value() == 0) {
            return;
        }
        List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();

        // - create map of normalized scores results returned from the single shard
        Map<Integer, float[]> normalizedScoresPerDoc = getNormalizedScoresPerDocument(topDocsPerSubQuery);

        // - create map of combined scores per doc id
        Map<Integer, Float> combinedNormalizedScoresByDocId = combineScoresAndGetCombinedNormalizedScoresPerDocument(
            normalizedScoresPerDoc,
            scoreCombinationTechnique
        );

        // - sort documents by scores and take first "max number" of docs
        // create a collection of doc ids that are sorted by their combined scores
        Collection<Integer> sortedDocsIds = getSortedDocsIds(compoundQueryTopDocs, sort, combinedNormalizedScoresByDocId);

        // - update query search results with combined scores
        updateQueryTopDocsWithCombinedScores(
            compoundQueryTopDocs,
            topDocsPerSubQuery,
            combinedNormalizedScoresByDocId,
            sortedDocsIds,
            getDocIdSortFieldsMap(compoundQueryTopDocs, combinedNormalizedScoresByDocId, sort),
            sort != null
        );
    }

    private boolean isSortOrderByScore(Sort sort) {
        if (sort == null) {
            return false;
        }

        for (SortField sortField : sort.getSort()) {
            if (SortField.Type.SCORE.equals(sortField.getType())) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param sort sort criteria
     * @param topDocsPerSubQuery top docs per subquery
     * @return list of top field docs which is deduced by typcasting top docs to top field docs.
     */
    private List<TopFieldDocs> getTopFieldDocs(final Sort sort, final List<TopDocs> topDocsPerSubQuery) {
        if (sort == null) {
            return null;
        }
        List<TopFieldDocs> topFieldDocs = new ArrayList<>();
        for (TopDocs topDocs : topDocsPerSubQuery) {
            // Check for scoreDocs length.
            // If scoreDocs length=0 then it means that no results are found for that particular subquery.
            if (topDocs.scoreDocs.length != 0) {
                topFieldDocs.add((TopFieldDocs) topDocs);
            }
        }
        return topFieldDocs;
    }

    /**
     * @param compoundTopDocs top docs that represent on shard
     * @param combinedNormalizedScoresByDocId docId to normalized scores map
     * @param sort sort criteria
     * @return map of docId and sort fields if sorting is enabled.
     */
    private Map<Integer, Object[]> getDocIdSortFieldsMap(
        final CompoundTopDocs compoundTopDocs,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final Sort sort
    ) {
        // If sort is null then no sort fields present therefore return null.
        if (sort == null) {
            return null;
        }
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        Map<Integer, Object[]> docIdSortFieldMap = new HashMap<>();
        final List<TopDocs> topFieldDocs = compoundTopDocs.getTopDocs();
        final boolean isSortByScore = isSortOrderByScore(sort);
        for (TopDocs topDocs : topFieldDocs) {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                FieldDoc fieldDoc = (FieldDoc) scoreDoc;

                if (docIdSortFieldMap.get(fieldDoc.doc) == null) {
                    // If sort by score then replace sort field value with normalized score.
                    if (isSortByScore) {
                        docIdSortFieldMap.put(fieldDoc.doc, new Object[] { combinedNormalizedScoresByDocId.get(fieldDoc.doc) });
                    } else {
                        docIdSortFieldMap.put(fieldDoc.doc, fieldDoc.fields);
                    }
                }
            }
        }
        return docIdSortFieldMap;
    }

    private List<Integer> getSortedDocIds(final Map<Integer, Float> combinedNormalizedScoresByDocId) {
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        List<Integer> sortedDocsIds = new ArrayList<>(combinedNormalizedScoresByDocId.keySet());
        sortedDocsIds.sort((a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a)));
        return sortedDocsIds;
    }

    private Set<Integer> getSortedDocIdsBySortCriteria(final List<TopFieldDocs> topFieldDocs, final Sort sort) {
        if (Objects.isNull(topFieldDocs)) {
            throw new IllegalArgumentException("topFieldDocs cannot be null when sorting is enabled.");
        }
        // size will be equal to the number of score docs
        int size = 0;
        for (TopFieldDocs topFieldDoc : topFieldDocs) {
            size += topFieldDoc.scoreDocs.length;
        }

        // Merge the sorted results of individual queries to form a one final result per shard which is sorted.
        // Input
        // < 0, 0.7, shardId, [90]> //Query 1` result scoreDoc
        // < 1, 0.7, shardId, [70]> //Query 1 result scoreDoc
        // < 2, 0.3, shardId, [100]> //Query 2 result scoreDoc
        // < 1, 0.3, shardId, [70]> //Query 2 result scoreDoc

        // Output
        // < 2, 0.3, shardId, [100]>
        // < 0, 0.7, shardId, [90]>
        // < 1, 0.7, shardId, [70]>
        // < 1, 0.3, shardId, [70]>
        final TopDocs sortedTopDocs = TopDocs.merge(sort, 0, size, topFieldDocs.toArray(new TopFieldDocs[0]), SORTING_TIE_BREAKER);

        // Remove duplicates from the sorted top docs.
        Set<Integer> uniqueDocIds = new LinkedHashSet<>();
        for (ScoreDoc scoreDoc : sortedTopDocs.scoreDocs) {
            uniqueDocIds.add(scoreDoc.doc);
        }
        return uniqueDocIds;
    }

    private List<ScoreDoc> getCombinedScoreDocs(
        final CompoundTopDocs compoundQueryTopDocs,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final Collection<Integer> sortedScores,
        final long maxHits,
        final Map<Integer, Object[]> docIdSortFieldMap,
        boolean isSortingEnabled
    ) {
        // ShardId will be -1 when index has multiple shards
        int shardId = -1;
        // ShardId will not be -1 in when index has single shard because Fetch phase gets executed before Normalization
        if (!compoundQueryTopDocs.getScoreDocs().isEmpty()) {
            shardId = compoundQueryTopDocs.getScoreDocs().get(0).shardIndex;
        }
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        int hitCount = 0;
        for (Integer docId : sortedScores) {
            if (hitCount == maxHits) {
                break;
            }
            scoreDocs.add(getScoreDoc(isSortingEnabled, docId, shardId, combinedNormalizedScoresByDocId, docIdSortFieldMap));
            hitCount++;
        }
        return scoreDocs;
    }

    private ScoreDoc getScoreDoc(
        final boolean isSortEnabled,
        final int docId,
        final int shardId,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final Map<Integer, Object[]> docIdSortFieldMap
    ) {
        if (isSortEnabled && docIdSortFieldMap != null) {
            return new FieldDoc(docId, combinedNormalizedScoresByDocId.get(docId), docIdSortFieldMap.get(docId), shardId);
        }
        return new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId);
    }

    public Map<Integer, float[]> getNormalizedScoresPerDocument(final List<TopDocs> topDocsPerSubQuery) {
        Map<Integer, float[]> normalizedScoresPerDoc = new HashMap<>();
        for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
            TopDocs topDocs = topDocsPerSubQuery.get(j);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                normalizedScoresPerDoc.computeIfAbsent(scoreDoc.doc, key -> {
                    float[] scores = new float[topDocsPerSubQuery.size()];
                    // we initialize with -1.0, as after normalization it's possible that score is 0.0
                    return scores;
                });
                normalizedScoresPerDoc.get(scoreDoc.doc)[j] = scoreDoc.score;
            }
        }
        return normalizedScoresPerDoc;
    }

    private Map<Integer, Float> combineScoresAndGetCombinedNormalizedScoresPerDocument(
        final Map<Integer, float[]> normalizedScoresPerDocument,
        final ScoreCombinationTechnique scoreCombinationTechnique
    ) {
        return normalizedScoresPerDocument.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
    }

    private void updateQueryTopDocsWithCombinedScores(
        final CompoundTopDocs compoundQueryTopDocs,
        final List<TopDocs> topDocsPerSubQuery,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final Collection<Integer> sortedScores,
        Map<Integer, Object[]> docIdSortFieldMap,
        boolean isSortingEnabled
    ) {
        // - max number of hits will be the same which are passed from QueryPhase
        long maxHits = compoundQueryTopDocs.getTotalHits().value();
        // - update query search results with normalized scores
        compoundQueryTopDocs.setScoreDocs(
            getCombinedScoreDocs(
                compoundQueryTopDocs,
                combinedNormalizedScoresByDocId,
                sortedScores,
                maxHits,
                docIdSortFieldMap,
                isSortingEnabled
            )
        );
        compoundQueryTopDocs.setTotalHits(getTotalHits(topDocsPerSubQuery, maxHits));
    }

    private TotalHits getTotalHits(final List<TopDocs> topDocsPerSubQuery, final long maxHits) {
        TotalHits.Relation totalHits = TotalHits.Relation.EQUAL_TO;
        if (topDocsPerSubQuery.stream().anyMatch(topDocs -> topDocs.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)) {
            totalHits = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        return new TotalHits(maxHits, totalHits);
    }

    /**
     * Explain the score combination technique for each document in the given queryTopDocs.
     * @param queryTopDocs
     * @param combinationTechnique
     * @param sort
     * @return a map of SearchShard and List of ExplainationDetails for each document
     */
    public Map<SearchShard, List<ExplanationDetails>> explain(
        final List<CompoundTopDocs> queryTopDocs,
        final ScoreCombinationTechnique combinationTechnique,
        final Sort sort
    ) {
        // In case of duplicate keys, keep the first value
        Map<SearchShard, List<ExplanationDetails>> explanations = new HashMap<>();
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            explanations.putIfAbsent(
                compoundQueryTopDocs.getSearchShard(),
                explainByShard(combinationTechnique, compoundQueryTopDocs, sort)
            );
        }
        return explanations;
    }

    private List<ExplanationDetails> explainByShard(
        final ScoreCombinationTechnique scoreCombinationTechnique,
        final CompoundTopDocs compoundQueryTopDocs,
        final Sort sort
    ) {
        if (Objects.isNull(compoundQueryTopDocs) || compoundQueryTopDocs.getTotalHits().value() == 0) {
            return List.of();
        }
        // create map of normalized scores results returned from the single shard
        Map<Integer, float[]> normalizedScoresPerDoc = getNormalizedScoresPerDocument(compoundQueryTopDocs.getTopDocs());
        // combine scores
        Map<Integer, Float> combinedNormalizedScoresByDocId = normalizedScoresPerDoc.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
        // sort combined scores as per sorting criteria - either score desc or field sorting
        Collection<Integer> sortedDocsIds = getSortedDocsIds(compoundQueryTopDocs, sort, combinedNormalizedScoresByDocId);

        List<ExplanationDetails> listOfExplanations = new ArrayList<>();
        String combinationDescription = String.format(
            Locale.ROOT,
            "%s combination of:",
            ((ExplainableTechnique) scoreCombinationTechnique).describe()
        );
        for (int docId : sortedDocsIds) {
            ExplanationDetails explanation = new ExplanationDetails(
                docId,
                List.of(
                    MinMaxScoreNormalizationTechnique.MutablePair.of(combinedNormalizedScoresByDocId.get(docId), combinationDescription)
                )
            );
            listOfExplanations.add(explanation);
        }
        return listOfExplanations;
    }

    private Collection<Integer> getSortedDocsIds(
        final CompoundTopDocs compoundQueryTopDocs,
        final Sort sort,
        final Map<Integer, Float> combinedNormalizedScoresByDocId
    ) {
        Collection<Integer> sortedDocsIds;
        if (sort != null) {
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            sortedDocsIds = getSortedDocIdsBySortCriteria(getTopFieldDocs(sort, topDocsPerSubQuery), sort);
        } else {
            sortedDocsIds = getSortedDocIds(combinedNormalizedScoresByDocId);
        }
        return sortedDocsIds;
    }
}

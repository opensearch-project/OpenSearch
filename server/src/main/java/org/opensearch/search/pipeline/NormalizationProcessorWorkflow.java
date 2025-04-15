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
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.SortedWiderNumericSortField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class abstracts steps required for score normalization and combination, this includes pre-processing of incoming data
 * and post-processing of final results
 */
public class NormalizationProcessorWorkflow {
    private static final Logger logger = LogManager.getLogger(NormalizationProcessorWorkflow.class);
    public static final Float MAX_SCORE_WHEN_NO_HITS_FOUND = 0.0f;
    public static final String EXPLANATION_RESPONSE_KEY = "explanation_response";

    public NormalizationProcessorWorkflow(ScoreNormalizer scoreNormalizer, ScoreCombiner scoreCombiner) {
        this.scoreNormalizer = scoreNormalizer;
        this.scoreCombiner = scoreCombiner;
    }

    private final ScoreNormalizer scoreNormalizer;
    private final ScoreCombiner scoreCombiner;

    /**
     * Start execution of this workflow
     * @param request contains querySearchResults input data with QuerySearchResult
     * from multiple shards, fetchSearchResultOptional, normalizationTechnique technique for score normalization
     *  combinationTechnique technique for score combination, and nullable rankConstant only used in RRF technique
     */
    public void execute(final NormalizationProcessorWorkflowExecuteRequest request) {
        List<QuerySearchResult> querySearchResults = request.getQuerySearchResults();
        Optional<FetchSearchResult> fetchSearchResultOptional = request.getFetchSearchResultOptional();
        List<Integer> unprocessedDocIds = unprocessedDocIds(querySearchResults);

        // pre-process data
        logger.debug("Pre-process query results");
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        explain(request, queryTopDocs);

        // Data transfer object for score normalization used to pass nullable rankConstant which is only used in RRF
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(queryTopDocs)
            .normalizationTechnique(request.getNormalizationTechnique())
            .build();

        // normalize
        logger.debug("Do score normalization");
        scoreNormalizer.normalizeScores(normalizeScoresDTO);

        CombineScoresDto combineScoresDTO = CombineScoresDto.builder()
            .queryTopDocs(queryTopDocs)
            .scoreCombinationTechnique(request.getCombinationTechnique())
            .querySearchResults(querySearchResults)
            .sort(evaluateSortCriteria(querySearchResults, queryTopDocs))
            .fromValueForSingleShard(getFromValueIfSingleShard(request))
            .build();

        // combine
        logger.debug("Do score combination");
        scoreCombiner.combineScores(combineScoresDTO);

        // post-process data
        logger.debug("Post-process query results after score normalization and combination");
        updateOriginalQueryResults(combineScoresDTO, fetchSearchResultOptional.isPresent());
        updateOriginalFetchResults(
            querySearchResults,
            fetchSearchResultOptional,
            unprocessedDocIds,
            combineScoresDTO.getFromValueForSingleShard()
        );
    }

    /**
     * Get value of from parameter when there is a single shard
     * and fetch phase is already executed
     * Ref https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/SearchService.java#L715
     */
    private int getFromValueIfSingleShard(final NormalizationProcessorWorkflowExecuteRequest request) {
        final SearchPhaseContext searchPhaseContext = request.getSearchPhaseContext();
        if (searchPhaseContext.getNumShards() > 1 || request.fetchSearchResultOptional.isEmpty()) {
            return -1;
        }
        int from = searchPhaseContext.getRequest().source().from();
        // for the initial searchRequest, it creates a default search context which sets the value of
        // from to 0 if it's -1. That's not the case with SearchPhaseContext, that's why need to
        // explicitly set to 0 for the single shard case
        // Ref:
        // https://github.com/opensearch-project/OpenSearch/blob/2.18/server/src/main/java/org/opensearch/search/DefaultSearchContext.java#L288
        if (from == -1) {
            return 0;
        }
        return from;
    }

    /**
     * Collects explanations from normalization and combination techniques and save thme into pipeline context. Later that
     * information will be read by the response processor to add it to search response
     */
    private void explain(NormalizationProcessorWorkflowExecuteRequest request, List<CompoundTopDocs> queryTopDocs) {
        if (!request.isExplain()) {
            return;
        }
        // build final result object with all explain related information
        if (Objects.nonNull(request.getPipelineProcessingContext())) {
            Sort sortForQuery = evaluateSortCriteria(request.getQuerySearchResults(), queryTopDocs);
            Map<DocIdAtSearchShard, ExplanationDetails> normalizationExplain = scoreNormalizer.explain(
                queryTopDocs,
                (ExplainableTechnique) request.getNormalizationTechnique()
            );
            Map<SearchShard, List<ExplanationDetails>> combinationExplain = scoreCombiner.explain(
                queryTopDocs,
                request.getCombinationTechnique(),
                sortForQuery
            );
            Map<SearchShard, List<CombinedExplanationDetails>> combinedExplanations = new HashMap<>();
            for (Map.Entry<SearchShard, List<ExplanationDetails>> entry : combinationExplain.entrySet()) {
                List<CombinedExplanationDetails> combinedDetailsList = new ArrayList<>();
                for (ExplanationDetails explainDetail : entry.getValue()) {
                    DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(explainDetail.getDocId(), entry.getKey());
                    CombinedExplanationDetails combinedDetail = CombinedExplanationDetails.builder()
                        .normalizationExplanations(normalizationExplain.get(docIdAtSearchShard))
                        .combinationExplanations(explainDetail)
                        .build();
                    combinedDetailsList.add(combinedDetail);
                }
                combinedExplanations.put(entry.getKey(), combinedDetailsList);
            }

            ExplanationPayload explanationPayload = ExplanationPayload.builder()
                .explainPayload(Map.of(ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR, combinedExplanations))
                .build();
            // store explain object to pipeline context
            PipelineProcessingContext pipelineProcessingContext = request.getPipelineProcessingContext();
            pipelineProcessingContext.setAttribute(EXPLANATION_RESPONSE_KEY, explanationPayload);
        }
    }

    /**
     * Getting list of CompoundTopDocs from list of QuerySearchResult. Each CompoundTopDocs is for individual shard
     * @param querySearchResults collection of QuerySearchResult for all shards
     * @return collection of CompoundTopDocs, one object for each shard
     */
    private List<CompoundTopDocs> getQueryTopDocs(final List<QuerySearchResult> querySearchResults) {
        List<CompoundTopDocs> queryTopDocs = querySearchResults.stream()
            .filter(searchResult -> Objects.nonNull(searchResult.topDocs()))
            .map(CompoundTopDocs::new)
            .collect(Collectors.toList());
        if (queryTopDocs.size() != querySearchResults.size()) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "query results were not formatted correctly by the hybrid query; sizes of querySearchResults [%d] and queryTopDocs [%d] must match",
                    querySearchResults.size(),
                    queryTopDocs.size()
                )
            );
        }
        return queryTopDocs;
    }

    private void updateOriginalQueryResults(final CombineScoresDto combineScoresDTO, final boolean isFetchPhaseExecuted) {
        final List<QuerySearchResult> querySearchResults = combineScoresDTO.getQuerySearchResults();
        final List<CompoundTopDocs> queryTopDocs = getCompoundTopDocs(combineScoresDTO, querySearchResults);
        final Sort sort = combineScoresDTO.getSort();
        int totalScoreDocsCount = 0;
        for (int index = 0; index < querySearchResults.size(); index++) {
            QuerySearchResult querySearchResult = querySearchResults.get(index);
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(index);
            totalScoreDocsCount += updatedTopDocs.getScoreDocs().size();
            TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(
                buildTopDocs(updatedTopDocs, sort),
                maxScoreForShard(updatedTopDocs, sort != null)
            );
            // Fetch Phase had ran before the normalization phase, therefore update the from value in result of each shard.
            // This will ensure the trimming of the search results.
            if (isFetchPhaseExecuted) {
                querySearchResult.from(combineScoresDTO.getFromValueForSingleShard());
            }
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, querySearchResult.sortValueFormats());
        }

        final int from = querySearchResults.get(0).from();
        if (from > totalScoreDocsCount) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Reached end of search result, increase pagination_depth value to see more results")
            );
        }
    }

    private List<CompoundTopDocs> getCompoundTopDocs(CombineScoresDto combineScoresDTO, List<QuerySearchResult> querySearchResults) {
        final List<CompoundTopDocs> queryTopDocs = combineScoresDTO.getQueryTopDocs();
        if (querySearchResults.size() != queryTopDocs.size()) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "query results were not formatted correctly by the hybrid query; sizes of querySearchResults [%d] and queryTopDocs [%d] must match",
                    querySearchResults.size(),
                    queryTopDocs.size()
                )
            );
        }
        return queryTopDocs;
    }

    /**
     * Get Max score on Shard
     * @param updatedTopDocs updatedTopDocs compound top docs on a shard
     * @param isSortEnabled if sort is enabled or disabled
     * @return  max score
     */
    private float maxScoreForShard(CompoundTopDocs updatedTopDocs, boolean isSortEnabled) {
        if (updatedTopDocs.getTotalHits().value() == 0 || updatedTopDocs.getScoreDocs().isEmpty()) {
            return MAX_SCORE_WHEN_NO_HITS_FOUND;
        }
        if (isSortEnabled) {
            float maxScore = MAX_SCORE_WHEN_NO_HITS_FOUND;
            // In case of sorting iterate over score docs and deduce the max score
            for (ScoreDoc scoreDoc : updatedTopDocs.getScoreDocs()) {
                maxScore = Math.max(maxScore, scoreDoc.score);
            }
            return maxScore;
        }
        // If it is a normal hybrid query then first entry of score doc will have max score
        return updatedTopDocs.getScoreDocs().get(0).score;
    }

    /**
     * Get Top Docs on Shard
     * @param updatedTopDocs compound top docs on a shard
     * @param sort  sort criteria
     * @return TopDocs which will be instance of TopFieldDocs  if sort is enabled.
     */
    private TopDocs buildTopDocs(CompoundTopDocs updatedTopDocs, Sort sort) {
        if (sort != null) {
            return new TopFieldDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new FieldDoc[0]), sort.getSort());
        }
        return new TopDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new ScoreDoc[0]));
    }

    /**
     * A workaround for a single shard case, fetch has happened, and we need to update both fetch and query results
     */
    private void updateOriginalFetchResults(
        final List<QuerySearchResult> querySearchResults,
        final Optional<FetchSearchResult> fetchSearchResultOptional,
        final List<Integer> docIds,
        final int fromValueForSingleShard
    ) {
        if (fetchSearchResultOptional.isEmpty()) {
            return;
        }
        // fetch results have list of document content, that includes start/stop and
        // delimiter elements. list is in original order from query searcher. We need to:
        // 1. filter out start/stop and delimiter elements
        // 2. filter out duplicates from different sub-queries
        // 3. update original scores to normalized and combined values
        // 4. order scores based on normalized and combined values
        FetchSearchResult fetchSearchResult = fetchSearchResultOptional.get();
        // checking case when results are cached
        boolean requestCache = Objects.nonNull(querySearchResults)
            && !querySearchResults.isEmpty()
            && Objects.nonNull(querySearchResults.get(0).getShardSearchRequest().requestCache())
            && querySearchResults.get(0).getShardSearchRequest().requestCache();

        SearchHit[] searchHitArray = getSearchHits(docIds, fetchSearchResult, requestCache);

        // create map of docId to index of search hits. This solves (2), duplicates are from
        // delimiter and start/stop elements, they all have same valid doc_id. For this map
        // we use doc_id as a key, and all those special elements are collapsed into a single
        // key-value pair.
        Map<Integer, SearchHit> docIdToSearchHit = new HashMap<>();
        for (int i = 0; i < searchHitArray.length; i++) {
            int originalDocId = docIds.get(i);
            docIdToSearchHit.put(originalDocId, searchHitArray[i]);
        }

        QuerySearchResult querySearchResult = querySearchResults.get(0);
        TopDocs topDocs = querySearchResult.topDocs().topDocs;
        // Scenario to handle when calculating the trimmed length of updated search hits
        // When normalization process runs after fetch phase, then search hits already fetched. Therefore, use the from value sent in the
        // search request to calculate the effective length of updated search hits array.
        int trimmedLengthOfSearchHits = topDocs.scoreDocs.length - fromValueForSingleShard;
        // iterate over the normalized/combined scores, that solves (1) and (3)
        SearchHit[] updatedSearchHitArray = new SearchHit[trimmedLengthOfSearchHits];
        for (int i = 0; i < trimmedLengthOfSearchHits; i++) {
            // Read topDocs after the desired from length
            ScoreDoc scoreDoc = topDocs.scoreDocs[i + fromValueForSingleShard];
            // get fetched hit content by doc_id
            SearchHit searchHit = docIdToSearchHit.get(scoreDoc.doc);
            // update score to normalized/combined value (3)
            searchHit.score(scoreDoc.score);
            updatedSearchHitArray[i] = searchHit;
        }
        SearchHits updatedSearchHits = new SearchHits(
            updatedSearchHitArray,
            querySearchResult.getTotalHits(),
            querySearchResult.getMaxScore()
        );
        fetchSearchResult.hits(updatedSearchHits);
    }

    private SearchHit[] getSearchHits(final List<Integer> docIds, final FetchSearchResult fetchSearchResult, final boolean requestCache) {
        SearchHits searchHits = fetchSearchResult.hits();
        SearchHit[] searchHitArray = searchHits.getHits();
        // validate the both collections are of the same size
        if (Objects.isNull(searchHitArray)) {
            throw new IllegalStateException(
                "score normalization processor cannot produce final query result, fetch query phase returns empty results"
            );
        }
        // in case of cached request results of fetch and query may be different, only restriction is
        // that number of query results size is greater or equal size of fetch results
        if ((!requestCache && searchHitArray.length != docIds.size()) || requestCache && docIds.size() < searchHitArray.length) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "score normalization processor cannot produce final query result, the number of documents after fetch phase [%d] is different from number of documents from query phase [%d]",
                    searchHitArray.length,
                    docIds.size()
                )
            );
        }
        return searchHitArray;
    }

    private List<Integer> unprocessedDocIds(final List<QuerySearchResult> querySearchResults) {
        List<Integer> docIds = querySearchResults.isEmpty()
            ? List.of()
            : Arrays.stream(querySearchResults.get(0).topDocs().topDocs.scoreDocs)
                .map(scoreDoc -> scoreDoc.doc)
                .collect(Collectors.toList());
        return docIds;
    }

    /**
     * @param querySearchResults list of query search results where each search result represents a result from the shard.
     * @param queryTopDocs list of top docs which have results with top scores.
     * @return sort criteria
     */
    public static Sort evaluateSortCriteria(final List<QuerySearchResult> querySearchResults, final List<CompoundTopDocs> queryTopDocs) {
        if (!checkIfSortEnabled(querySearchResults)) {
            return null;
        }
        return createSort(getTopFieldDocs(queryTopDocs));
    }

    // Check if sort is enabled by checking docValueFormats Object
    private static boolean checkIfSortEnabled(final List<QuerySearchResult> querySearchResults) {
        if (querySearchResults == null || querySearchResults.isEmpty() || querySearchResults.get(0) == null) {
            throw new IllegalArgumentException("shard results cannot be null in the normalization process.");
        }
        return querySearchResults.get(0).sortValueFormats() != null;
    }

    /**
     * Creates Sort object from topFieldsDocs fields.
     * It is necessary to widen the SortField.Type to maximum byte size for merging sorted docs.
     * Different indices might have different types. This will avoid user to do re-index of data
     * in case of mapping field change for newly indexed data.
     * This will support Int to Long and Float to Double.
     * Earlier widening of type was taken care in IndexNumericFieldData, but since we now want to
     * support sort optimization, we removed type widening there and taking care here during merging.
     * More details here https://github.com/opensearch-project/OpenSearch/issues/6326
     */
    private static Sort createSort(TopFieldDocs[] topFieldDocs) {
        final SortField[] firstTopDocFields = topFieldDocs[0].fields;
        final SortField[] newFields = new SortField[firstTopDocFields.length];

        for (int i = 0; i < firstTopDocFields.length; i++) {
            final SortField delegate = firstTopDocFields[i];
            final SortField.Type sortFieldType = delegate instanceof SortedNumericSortField
                ? ((SortedNumericSortField) delegate).getNumericType()
                : delegate.getType();

            if (SortedWiderNumericSortField.isTypeSupported(sortFieldType) && isSortWideningRequired(topFieldDocs, i)) {
                newFields[i] = new SortedWiderNumericSortField(delegate.getField(), sortFieldType, delegate.getReverse());
            } else {
                newFields[i] = firstTopDocFields[i];
            }
        }
        return new Sort(newFields);
    }

    /**
     * It will compare respective SortField between shards to see if any shard results have different
     * field mapping type, accordingly it will decide to widen the sort fields.
     */
    private static boolean isSortWideningRequired(TopFieldDocs[] topFieldDocs, int sortFieldindex) {
        for (int i = 0; i < topFieldDocs.length - 1; i++) {
            TopFieldDocs currentTopFieldDoc = topFieldDocs[i];
            TopFieldDocs nextTopFieldDoc = topFieldDocs[i + 1];
            if (currentTopFieldDoc == null || nextTopFieldDoc == null) {
                throw new IllegalArgumentException("topFieldDocs cannot be null when sorting is applied");
            }
            if (!currentTopFieldDoc.fields[sortFieldindex].equals(nextTopFieldDoc.fields[sortFieldindex])) {
                return true;
            }
        }
        return false;
    }

    // Get the topFieldDocs array from the first shard result
    private static TopFieldDocs[] getTopFieldDocs(final List<CompoundTopDocs> queryTopDocs) {
        // loop over queryTopDocs and return the first set of topFieldDocs found
        // Considering the topDocs can be empty if no result is found on the shard therefore we need iterate over all the shards .
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            if (compoundTopDocs == null) {
                throw new IllegalArgumentException("CompoundTopDocs cannot be null in the normalization process");
            }
            if (containsTopFieldDocs(compoundTopDocs.getTopDocs())) {
                return compoundTopDocs.getTopDocs().toArray(new TopFieldDocs[0]);
            }
        }
        return new TopFieldDocs[0];
    }

    private static boolean containsTopFieldDocs(List<TopDocs> topDocs) {
        // topDocs can be empty if no results found in the shard
        if (topDocs == null || topDocs.isEmpty()) {
            return false;
        }
        for (TopDocs topDoc : topDocs) {
            if (topDoc != null && topDoc instanceof TopFieldDocs) {
                return true;
            }
        }
        return false;
    }
}

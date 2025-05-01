/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.opensearch.common.Nullable;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.search.profile.query.CollectorResult.REASON_SEARCH_TOP_HITS;

/**
 * HybridCollectorContext class
 */
public class HybridCollectorContext extends QueryCollectorContext {
    protected final @Nullable SortAndFormats sortAndFormats;
    private final int numHits;
    private final int trackTotalHitsUpTo;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final @Nullable Weight filterWeight;
    private final TopDocsMerger topDocsMerger;
    private final @Nullable FieldDoc after;
    private final SearchContext searchContext;
    private final Collector collector;
    private HybridCollectorManager manager;

    public static HybridCollectorContext createHybridCollectorContext(final SearchContext searchContext) throws IOException {
        if (searchContext.scrollContext() != null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Scroll operation is not supported in hybrid query"));
        }
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final int totalNumDocs = Math.max(0, reader.numDocs());
        int numDocs = Math.min(getSubqueryResultsRetrievalSize(searchContext), totalNumDocs);
        Weight filteringWeight = null;
        // Check for post filter to create weight for filter query and later use that weight in the search workflow
        if (Objects.nonNull(searchContext.parsedPostFilter()) && Objects.nonNull(searchContext.parsedPostFilter().query())) {
            Query filterQuery = searchContext.parsedPostFilter().query();
            ContextIndexSearcher searcher = searchContext.searcher();
            // ScoreMode COMPLETE_NO_SCORES will be passed as post_filter does not contribute in scoring. COMPLETE_NO_SCORES means it is not
            // a scoring clause
            // Boost factor 1f is taken because if boost is multiplicative of 1 then it means "no boost"
            // Previously this code in OpenSearch looked like
            // https://github.com/opensearch-project/OpenSearch/commit/36a5cf8f35e5cbaa1ff857b5a5db8c02edc1a187
            filteringWeight = searcher.createWeight(searcher.rewrite(filterQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        SortAndFormats sort = searchContext.sort();
        if (sort != null) {
            validateSortCriteria(searchContext, searchContext.trackScores());
        }

        boolean isSingleShard = searchContext.numberOfShards() == 1;
        // In case of single shard, it can happen that fetch phase might execute before normalization phase. Moreover, The pagination logic
        // lies in the fetch phase.
        // If the fetch phase gets executed before the normalization phase, then the result will be not paginated as per normalized score.
        // Therefore, to avoid it we will update from value in search context to 0. This will stop fetch phase to trim results prematurely.
        // Later in the normalization phase we will update QuerySearchResult object with the right from value, to handle the effective
        // trimming of results.
        if (isSingleShard && searchContext.from() > 0) {
            searchContext.from(0);
        }

        HitsThresholdChecker hitsThreshold = new HitsThresholdChecker(Math.max(numDocs, searchContext.trackTotalHitsUpTo()));
        // HybridTopScoreDocCollector hybridCollector = new HybridTopScoreDocCollector(numDocs, hitsThreshold);
        // Collector hcollector = Objects.nonNull(filteringWeight) ? new FilteredCollector(hybridCollector, filteringWeight) :
        // hybridCollector;
        return new HybridCollectorContext(
            REASON_SEARCH_TOP_HITS,
            sort,
            numDocs,
            searchContext.trackTotalHitsUpTo(),
            hitsThreshold,
            filteringWeight,
            new TopDocsMerger(searchContext.sort()),
            searchContext.searchAfter(),
            searchContext
        );
    }

    private static int getSubqueryResultsRetrievalSize(final SearchContext searchContext) {
        HybridQuery hybridQuery = unwrapHybridQuery(searchContext);
        Integer paginationDepth = hybridQuery.getQueryContext().getPaginationDepth();

        // Pagination is expected to work only when pagination_depth is provided in the search request.
        if (Objects.isNull(paginationDepth) && searchContext.from() > 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "pagination_depth param is missing in the search request"));
        }

        if (Objects.nonNull(paginationDepth)) {
            return paginationDepth;
        }

        // Switch to from+size retrieval size during standard hybrid query execution where from is 0.
        return searchContext.size();
    }

    private static HybridQuery unwrapHybridQuery(final SearchContext searchContext) {
        HybridQuery hybridQuery;
        Query query = searchContext.query();
        // In case of nested fields and alias filter, hybrid query is wrapped under bool query and lies in the first clause.
        if (isHybridQueryWrappedInBooleanQuery(searchContext, searchContext.query())) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            hybridQuery = (HybridQuery) booleanQuery.clauses().get(0).query();
        } else {
            hybridQuery = (HybridQuery) query;
        }
        return hybridQuery;
    }

    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        return ((hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && isWrappedHybridQuery(query)
            && !((BooleanQuery) query).clauses().isEmpty());
    }

    private static boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    private static boolean isWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.query() instanceof HybridQuery);
    }

    private static boolean hasAliasFilter(final Query query, final SearchContext searchContext) {
        return Objects.nonNull(searchContext.aliasFilter());
    }

    HybridCollectorContext(
        String profileName,
        @Nullable SortAndFormats sortAndFormats,
        int numHits,
        int trackTotalHitsUpTo,
        HitsThresholdChecker hitsThresholdChecker,
        @Nullable Weight filterWeight,
        TopDocsMerger topDocsMerger,
        @Nullable FieldDoc after,
        SearchContext searchContext
    ) {
        super(profileName);
        this.sortAndFormats = sortAndFormats;
        this.numHits = numHits;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.filterWeight = filterWeight;
        this.topDocsMerger = topDocsMerger;
        this.after = after;
        this.searchContext = searchContext;
        HybridCollectorManager hybridCollectorManager = new HybridCollectorManager(
            numHits,
            hitsThresholdChecker,
            trackTotalHitsUpTo,
            sortAndFormats,
            filterWeight,
            topDocsMerger,
            after,
            searchContext
        );
        searchContext.queryCollectorManagers().put(HybridCollectorManager.class, hybridCollectorManager);

        this.manager = hybridCollectorManager;
        this.collector = manager.newCollector();
    }

    @Override
    CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        this.manager = new HybridCollectorManager(
            numHits,
            hitsThresholdChecker,
            trackTotalHitsUpTo,
            sortAndFormats,
            filterWeight,
            topDocsMerger,
            after,
            searchContext
        );
        return manager;
    }
    //
    // TopDocsAndMaxScore newTopDocs() {
    // TopDocs in = topDocsSupplier.get();
    // float maxScore = maxScoreSupplier.get();
    // final TopDocs newTopDocs;
    // if (in instanceof TopFieldDocs) {
    // TopFieldDocs fieldDocs = (TopFieldDocs) in;
    // newTopDocs = new TopFieldDocs(totalHitsSupplier.get(), fieldDocs.scoreDocs, fieldDocs.fields);
    // } else {
    // newTopDocs = new TopDocs(totalHitsSupplier.get(), in.scoreDocs);
    // }
    // return new TopDocsAndMaxScore(newTopDocs, maxScore);
    // }

    private static void validateSortCriteria(SearchContext searchContext, boolean trackScores) {
        SortField[] sortFields = searchContext.sort().sort.getSort();
        boolean hasFieldSort = false;
        boolean hasScoreSort = false;
        for (SortField sortField : sortFields) {
            SortField.Type type = sortField.getType();
            if (type.equals(SortField.Type.SCORE)) {
                hasScoreSort = true;
            } else {
                hasFieldSort = true;
            }
            if (hasScoreSort && hasFieldSort) {
                break;
            }
        }
        if (hasScoreSort && hasFieldSort) {
            throw new IllegalArgumentException(
                "_score sort criteria cannot be applied with any other criteria. Please select one sort criteria out of them."
            );
        }
        if (trackScores && hasFieldSort) {
            throw new IllegalArgumentException(
                "Hybrid search results when sorted by any field, docId or _id, track_scores must be set to false."
            );
        }
        if (trackScores && hasScoreSort) {
            throw new IllegalArgumentException("Hybrid search results are by default sorted by _score, track_scores must be set to false.");
        }
    }

    public static boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery
            || (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery)) {
            return true;
        }
        return false;
    }

    @Override
    Collector create(Collector in) throws IOException {
        return collector;
    }

    @Override
    void postProcess(QuerySearchResult result) throws IOException {
        CollectorManager<Collector, ReduceableSearchResult> collectorManager = (CollectorManager<
            Collector,
            ReduceableSearchResult>) searchContext.queryCollectorManagers().get(HybridCollectorManager.class);
        final Collection<Collector> subCollectors = new ArrayList<>();
        subCollectors.add(collector);
        try {
            collectorManager.reduce(subCollectors).reduce(result);
        } catch (IOException e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "failed to execute hybrid query aggregation processor", e);
        }
    }

}

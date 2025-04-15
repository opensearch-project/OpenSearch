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
import org.apache.lucene.search.Sort;
import org.opensearch.common.Nullable;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;

import reactor.util.annotation.NonNull;

public class CombineScoresDto {
    private static final Logger log = LogManager.getLogger(CombineScoresDto.class);

    public CombineScoresDto(
        @NonNull List<CompoundTopDocs> queryTopDocs,
        @NonNull ScoreCombinationTechnique scoreCombinationTechnique,
        @NonNull List<QuerySearchResult> querySearchResults,
        @Nullable Sort sort,
        int fromValueForSingleShard
    ) {
        this.queryTopDocs = queryTopDocs;
        this.fromValueForSingleShard = fromValueForSingleShard;
        this.sort = sort;
        this.querySearchResults = querySearchResults;
        this.scoreCombinationTechnique = scoreCombinationTechnique;
    }

    public int getFromValueForSingleShard() {
        return fromValueForSingleShard;
    }

    @Nullable
    public Sort getSort() {
        return sort;
    }

    @NonNull
    public List<QuerySearchResult> getQuerySearchResults() {
        return querySearchResults;
    }

    @NonNull
    public ScoreCombinationTechnique getScoreCombinationTechnique() {
        return scoreCombinationTechnique;
    }

    @NonNull
    public List<CompoundTopDocs> getQueryTopDocs() {
        return queryTopDocs;
    }

    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @NonNull
    private List<QuerySearchResult> querySearchResults;
    @Nullable
    private Sort sort;
    private int fromValueForSingleShard;

    public static CombineScoresDtoBuilder builder() {
        return new CombineScoresDtoBuilder();
    }

    public static class CombineScoresDtoBuilder {
        private List<CompoundTopDocs> queryTopDocs;
        private ScoreCombinationTechnique scoreCombinationTechnique;
        private List<QuerySearchResult> querySearchResults;
        private Sort sort;
        private int fromValueForSingleShard;

        public CombineScoresDtoBuilder scoreCombinationTechnique(ScoreCombinationTechnique scoreCombinationTechnique) {
            this.scoreCombinationTechnique = scoreCombinationTechnique;
            return this;
        }

        public CombineScoresDtoBuilder querySearchResults(@NonNull List<QuerySearchResult> querySearchResults) {
            this.querySearchResults = querySearchResults;
            return this;
        }

        public CombineScoresDtoBuilder sort(@Nullable Sort sort) {
            this.sort = sort;
            return this;
        }

        public CombineScoresDtoBuilder fromValueForSingleShard(int fromValueForSingleShard) {
            this.fromValueForSingleShard = fromValueForSingleShard;
            return this;
        }

        public CombineScoresDtoBuilder queryTopDocs(List<CompoundTopDocs> queryTopDocs) {
            this.queryTopDocs = queryTopDocs;
            return this;
        }

        public CombineScoresDto build() {
            return new CombineScoresDto(queryTopDocs, scoreCombinationTechnique, querySearchResults, sort, fromValueForSingleShard);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.List;

import reactor.util.annotation.NonNull;

public class NormalizeScoresDTO {
    public NormalizeScoresDTO(@NonNull List<CompoundTopDocs> queryTopDocs, @NonNull ScoreNormalizationTechnique normalizationTechnique) {
        this.queryTopDocs = queryTopDocs;
        this.normalizationTechnique = normalizationTechnique;
    }

    @NonNull
    public List<CompoundTopDocs> getQueryTopDocs() {
        return queryTopDocs;
    }

    @NonNull
    public ScoreNormalizationTechnique getNormalizationTechnique() {
        return normalizationTechnique;
    }

    @NonNull
    private List<CompoundTopDocs> queryTopDocs;
    @NonNull
    private ScoreNormalizationTechnique normalizationTechnique;

    public static NormalizeScoresDTOBuilder builder() {
        return new NormalizeScoresDTOBuilder();
    }

    public static class NormalizeScoresDTOBuilder {
        private List<CompoundTopDocs> queryTopDocs;
        private ScoreNormalizationTechnique normalizationTechnique;

        public NormalizeScoresDTOBuilder queryTopDocs(List<CompoundTopDocs> queryTopDocs) {
            this.queryTopDocs = queryTopDocs;
            return this;
        }

        public NormalizeScoresDTOBuilder normalizationTechnique(ScoreNormalizationTechnique normalizationTechnique) {
            this.normalizationTechnique = normalizationTechnique;
            return this;
        }

        public NormalizeScoresDTO build() {
            return new NormalizeScoresDTO(queryTopDocs, normalizationTechnique);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import reactor.util.annotation.NonNull;

/**
 * DTO object to hold data required for validation.
 */
public class TechniqueCompatibilityCheckDTO {
    @NonNull
    public ScoreCombinationTechnique getScoreCombinationTechnique() {
        return scoreCombinationTechnique;
    }

    @NonNull
    public ScoreNormalizationTechnique getScoreNormalizationTechnique() {
        return scoreNormalizationTechnique;
    }

    public TechniqueCompatibilityCheckDTO(
        @NonNull ScoreCombinationTechnique scoreCombinationTechnique,
        @NonNull ScoreNormalizationTechnique scoreNormalizationTechnique
    ) {
        this.scoreCombinationTechnique = scoreCombinationTechnique;
        this.scoreNormalizationTechnique = scoreNormalizationTechnique;
    }

    @NonNull
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @NonNull
    private ScoreNormalizationTechnique scoreNormalizationTechnique;

    /*
    Builder method
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
    Builder class
     */
    public static class Builder {
        private ScoreCombinationTechnique scoreCombinationTechnique;
        private ScoreNormalizationTechnique scoreNormalizationTechnique;

        public Builder scoreCombinationTechnique(ScoreCombinationTechnique scoreCombinationTechnique) {
            this.scoreCombinationTechnique = scoreCombinationTechnique;
            return this;
        }

        public Builder scoreNormalizationTechnique(ScoreNormalizationTechnique scoreNormalizationTechnique) {
            this.scoreNormalizationTechnique = scoreNormalizationTechnique;
            return this;
        }

        public TechniqueCompatibilityCheckDTO build() {
            return new TechniqueCompatibilityCheckDTO(scoreCombinationTechnique, scoreNormalizationTechnique);
        }
    }
}

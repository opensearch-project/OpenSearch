/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

public class CombinedExplanationDetails {
    public CombinedExplanationDetails(ExplanationDetails normalizationExplanations, ExplanationDetails combinationExplanations) {
        this.normalizationExplanations = normalizationExplanations;
        this.combinationExplanations = combinationExplanations;
    }

    public ExplanationDetails getNormalizationExplanations() {
        return normalizationExplanations;
    }

    public ExplanationDetails getCombinationExplanations() {
        return combinationExplanations;
    }

    private ExplanationDetails normalizationExplanations;
    private ExplanationDetails combinationExplanations;

    public static CombineExplanationDetailsBuilder builder() {
        return new CombineExplanationDetailsBuilder();
    }

    public static class CombineExplanationDetailsBuilder {
        private ExplanationDetails normalizationExplanations;
        private ExplanationDetails combinationExplanations;

        public CombineExplanationDetailsBuilder normalizationExplanations(ExplanationDetails normalizationExplanations) {
            this.normalizationExplanations = normalizationExplanations;
            return this;
        }

        public CombineExplanationDetailsBuilder combinationExplanations(ExplanationDetails combinationExplanations) {
            this.combinationExplanations = combinationExplanations;
            return this;
        }

        public CombinedExplanationDetails build() {
            return new CombinedExplanationDetails(normalizationExplanations, combinationExplanations);
        }
    }
}

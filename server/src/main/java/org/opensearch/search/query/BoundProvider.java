/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.TopDocs;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Interface for providers that can calculate bounds for streaming search.
 * Different search modalities (text, vector, etc.) implement this to provide
 * domain-specific bound calculations.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface BoundProvider {

    /**
     * Calculate the current bound for the given search context.
     * Lower bounds indicate higher confidence in the current results.
     *
     * @param context The search context
     * @return The calculated bound (lower = more confident)
     */
    double calculateBound(SearchContext context);

    /**
     * Check if the current results are stable enough to emit.
     *
     * @param context The search context
     * @return true if results are stable
     */
    boolean isStable(SearchContext context);

    /**
     * Get the current progress percentage (0.0 to 1.0).
     *
     * @param context The search context
     * @return Progress as a percentage
     */
    double getProgress(SearchContext context);

    /**
     * Get the current phase of the search.
     *
     * @return The current search phase
     */
    SearchPhase getCurrentPhase();

    /**
     * Search phases for streaming search.
     */
    @ExperimentalApi
    enum SearchPhase {
        FIRST,      // Initial shard-level processing
        SECOND,     // Shard-level refinement
        GLOBAL      // Global coordinator processing
    }

    /**
     * Search context for bound calculations.
     */
    @ExperimentalApi
    interface SearchContext {
        /**
         * Get the number of documents processed so far.
         */
        int getDocCount();

        /**
         * Get the current top-K results.
         */
        TopDocs getTopDocs();

        /**
         * Get the current k-th score.
         */
        float getKthScore();

        /**
         * Get the maximum possible score.
         */
        float getMaxPossibleScore();

        /**
         * Get the search modality type.
         */
        String getModality();
    }
}

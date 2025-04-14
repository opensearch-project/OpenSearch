/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.ScoreMode;

import java.util.Locale;

/**
 *  Abstracts algorithm that allows early termination for the search flow if number of hits reached
 *  certain treshold
 */
public class HitsThresholdChecker {
    private int hitCount;

    private final int totalHitsThreshold;

    public HitsThresholdChecker(int totalHitsThreshold) {
        if (totalHitsThreshold < 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "totalHitsThreshold must be >= 0, got %d", totalHitsThreshold));
        }
        this.totalHitsThreshold = totalHitsThreshold;
    }

    public void incrementHitCount() {
        ++hitCount;
    }

    public boolean isThresholdReached() {
        return hitCount >= getTotalHitsThreshold();
    }

    public ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
    }

    public int getTotalHitsThreshold() {
        return totalHitsThreshold;
    }
}

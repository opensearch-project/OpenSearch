/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

/**
 * BucketCountThresholds type that holds the local (either shard level or request level) bucket count thresholds in minDocCount and requireSize fields.
 * Similar to {@link TermsAggregator.BucketCountThresholds} however only provides getters for the local members and no setters.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class LocalBucketCountThresholds {

    private final long minDocCount;
    private final int requiredSize;

    public LocalBucketCountThresholds(long localminDocCount, int localRequiredSize) {
        this.minDocCount = localminDocCount;
        this.requiredSize = localRequiredSize;
    }

    public int getRequiredSize() {
        return requiredSize;
    }

    public long getMinDocCount() {
        return minDocCount;
    }
}

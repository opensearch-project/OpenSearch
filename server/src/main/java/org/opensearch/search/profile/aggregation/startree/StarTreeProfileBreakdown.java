/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.aggregation.startree;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.ProfileMetric;
import org.opensearch.search.profile.ProfileMetricUtil;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@linkplain AbstractProfileBreakdown} customized to work with star tree precompute objects in aggregations.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public class StarTreeProfileBreakdown extends AbstractProfileBreakdown {
    public StarTreeProfileBreakdown() {
        this(ProfileMetricUtil.getStarTreeAggregationProfileMetrics());
    }

    public StarTreeProfileBreakdown(Collection<Supplier<ProfileMetric>> timers) {
        super(timers);
    }

    public String defaultType() {
        return "StarTree";
    }

    public String defaultDescription() {
        return "Pre-computation using star-tree index";
    }
}

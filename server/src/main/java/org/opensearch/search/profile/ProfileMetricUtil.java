/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.search.profile.aggregation.AggregationTimingType;
import org.opensearch.search.profile.fetch.FetchTimingType;
import org.opensearch.search.profile.query.QueryTimingType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * Utility class to provide profile metrics to breakdowns.
 */
public class ProfileMetricUtil {

    public static Collection<Supplier<ProfileMetric>> getQueryProfileMetrics(Collection<Supplier<ProfileMetric>> customProfileMetrics) {
        Collection<Supplier<ProfileMetric>> metrics = getDefaultQueryProfileMetrics();
        metrics.addAll(customProfileMetrics);
        return metrics;
    }

    public static Collection<Supplier<ProfileMetric>> getDefaultQueryProfileMetrics() {
        Collection<Supplier<ProfileMetric>> metrics = new ArrayList<>();
        for (QueryTimingType type : QueryTimingType.values()) {
            metrics.add(() -> new Timer(type.toString()));
        }
        return metrics;
    }

    public static Collection<Supplier<ProfileMetric>> getAggregationProfileMetrics() {
        Collection<Supplier<ProfileMetric>> metrics = new ArrayList<>();
        for (AggregationTimingType type : AggregationTimingType.values()) {
            metrics.add(() -> new Timer(type.toString()));
        }
        return metrics;
    }

    public static Collection<Supplier<ProfileMetric>> getFetchProfileMetrics() {
        Collection<Supplier<ProfileMetric>> metrics = new ArrayList<>();
        for (FetchTimingType type : FetchTimingType.values()) {
            metrics.add(() -> new Timer(type.toString()));
        }
        return metrics;
    }
}

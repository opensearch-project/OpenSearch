/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Max value aggregator for star tree
 *
 * @opensearch.experimental
 */
public class MaxValueAggregator extends MinMaxValueAggregator {

    public MaxValueAggregator(StarTreeNumericType starTreeNumericType) {
        super(MetricStat.MAX, starTreeNumericType);
    }

    @Override
    public Double performValueAggregation(Double aggregatedValue, Double segmentDocValue) {
        return Math.max(aggregatedValue, segmentDocValue);
    }
}

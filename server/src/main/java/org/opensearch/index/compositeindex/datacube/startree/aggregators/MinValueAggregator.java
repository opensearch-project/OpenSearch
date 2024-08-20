/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Min value aggregator for star tree
 *
 * @opensearch.experimental
 */
class MinValueAggregator extends StatelessDoubleValueAggregator {

    public MinValueAggregator(StarTreeNumericType starTreeNumericType) {
        super(starTreeNumericType, null);
    }

    @Override
    protected Double performValueAggregation(Double aggregatedValue, Double segmentDocValue) {
        return Math.min(aggregatedValue, segmentDocValue);
    }
}

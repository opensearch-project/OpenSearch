/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.mapper.FieldValueConverter;

/**
 * Max value aggregator for star tree
 *
 * @opensearch.experimental
 */
class MaxValueAggregator extends StatelessDoubleValueAggregator {

    public MaxValueAggregator(FieldValueConverter fieldValueConverter) {
        super(fieldValueConverter, null);
    }

    @Override
    protected Double performValueAggregation(Double aggregatedValue, Double segmentDocValue) {
        return Math.max(aggregatedValue, segmentDocValue);
    }
}

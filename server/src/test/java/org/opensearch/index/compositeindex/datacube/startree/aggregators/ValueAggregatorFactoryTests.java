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
import org.opensearch.test.OpenSearchTestCase;

public class ValueAggregatorFactoryTests extends OpenSearchTestCase {

    public void testGetValueAggregatorForSumType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricStat.SUM, StarTreeNumericType.LONG);
        assertNotNull(aggregator);
        assertEquals(SumValueAggregator.class, aggregator.getClass());
    }

    public void testGetAggregatedValueTypeForSumType() {
        StarTreeNumericType starTreeNumericType = ValueAggregatorFactory.getAggregatedValueType(MetricStat.SUM);
        assertEquals(SumValueAggregator.VALUE_AGGREGATOR_TYPE, starTreeNumericType);
    }
}

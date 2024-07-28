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
import org.junit.Before;

public abstract class AbstractValueAggregatorTests extends OpenSearchTestCase {

    private ValueAggregator aggregator;

    @Before
    public void setup() {
        aggregator = getValueAggregator();
    }

    public abstract ValueAggregator getValueAggregator();

    public abstract MetricStat getMetricStat();

    public abstract StarTreeNumericType getValueAggregatorType();

    public void testGetAggregationType() {
        assertEquals(getMetricStat().getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetAggregatedValueType() {
        assertEquals(getValueAggregatorType(), aggregator.getAggregatedValueType());
    }

    public void testGetInitialAggregatedValueForSegmentDocNullValue() {
        assertEquals(aggregator.getIdentityMetricValue(), aggregator.getInitialAggregatedValueForSegmentDocValue(null));
    }

    public void testMergeAggregatedNullValueAndSegmentNullValue() {
        assertEquals(aggregator.getIdentityMetricValue(), aggregator.mergeAggregatedValueAndSegmentValue(null, null));
    }

    public void testMergeAggregatedNullValues() {
        assertEquals(aggregator.getIdentityMetricValue(), aggregator.mergeAggregatedValues(null, null));
    }

    public void testGetInitialAggregatedNullValue() {
        assertEquals(aggregator.getIdentityMetricValue(), aggregator.getInitialAggregatedValue(null));
    }

    public void testGetInitialAggregatedValueForSegmentDocValue() {
        Long randomLong = randomLong();
        if (aggregator instanceof CountValueAggregator) {
            assertEquals(CountValueAggregator.DEFAULT_INITIAL_VALUE, aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong()));
        } else {
            assertEquals(randomLong.doubleValue(), aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong));
        }
    }
}

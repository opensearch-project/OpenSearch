/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractValueAggregatorTests extends OpenSearchTestCase {

    private ValueAggregator aggregator;
    protected StarTreeNumericType starTreeNumericType;

    public AbstractValueAggregatorTests(StarTreeNumericType starTreeNumericType) {
        this.starTreeNumericType = starTreeNumericType;
    }

    @Before
    public void setup() {
        aggregator = getValueAggregator(starTreeNumericType);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (StarTreeNumericType starTreeNumericType : StarTreeNumericType.values()) {
            parameters.add(new Object[] { starTreeNumericType });
        }
        return parameters;
    }

    public abstract ValueAggregator getValueAggregator(StarTreeNumericType starTreeNumericType);

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
        long randomLong = randomLong();
        assertEquals(
            starTreeNumericType.getDoubleValue(randomLong),
            aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong)
        );
    }
}

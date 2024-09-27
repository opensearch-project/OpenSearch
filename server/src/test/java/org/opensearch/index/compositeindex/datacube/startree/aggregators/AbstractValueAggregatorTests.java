/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.index.mapper.FieldValueConverter;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractValueAggregatorTests extends OpenSearchTestCase {

    private ValueAggregator aggregator;
    protected FieldValueConverter fieldValueConverter;

    public AbstractValueAggregatorTests(FieldValueConverter fieldValueConverter) {
        this.fieldValueConverter = fieldValueConverter;
    }

    @Before
    public void setup() {
        aggregator = getValueAggregator(fieldValueConverter);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (FieldValueConverter fieldValueConverter : NumberFieldMapper.NumberType.values()) {
            parameters.add(new Object[] { fieldValueConverter });
        }
        return parameters;
    }

    public abstract ValueAggregator getValueAggregator(FieldValueConverter fieldValueConverter);

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
        if (aggregator instanceof CountValueAggregator) {
            assertEquals(CountValueAggregator.DEFAULT_INITIAL_VALUE, aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong()));
        } else {
            assertEquals(fieldValueConverter.toDoubleValue(randomLong), aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong));
        }
    }
}

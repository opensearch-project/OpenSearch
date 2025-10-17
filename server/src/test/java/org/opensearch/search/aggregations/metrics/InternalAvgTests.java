/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalAvgTests extends InternalAggregationTestCase<InternalAvg> {

    @Override
    protected InternalAvg createTestInstance(String name, Map<String, Object> metadata) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        long count = frequently() ? randomNonNegativeLong() % 100000 : 0;
        return new InternalAvg(name, randomDoubleBetween(0, 100000, true), count, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalAvg reduced, List<InternalAvg> inputs) {
        double sum = 0;
        long counts = 0;
        for (InternalAvg in : inputs) {
            sum += in.getSum();
            counts += in.getCount();
        }
        assertEquals(counts, reduced.getCount());
        assertEquals(sum, reduced.getSum(), 0.0000001);
        assertEquals(sum / counts, reduced.value(), 0.0000001);
    }

    public void testSummationAccuracy() {
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifyAvgOfDoubles(values, 0.9, 0d);

        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifyAvgOfDoubles(values, sum / n, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifyAvgOfDoubles(double[] values, double expected, double delta) {
        List<InternalAggregation> aggregations = new ArrayList<>(values.length);
        for (double value : values) {
            aggregations.add(new InternalAvg("dummy1", value, 1, null, null));
        }
        InternalAvg internalAvg = new InternalAvg("dummy2", 0, 0, null, null);
        InternalAvg reduced = internalAvg.reduce(aggregations, null);
        assertEquals(expected, reduced.getValue(), delta);
    }

    @Override
    protected void assertFromXContent(InternalAvg avg, ParsedAggregation parsedAggregation) {
        ParsedAvg parsed = ((ParsedAvg) parsedAggregation);
        assertEquals(avg.getValue(), parsed.getValue(), Double.MIN_VALUE);
        // we don't print out VALUE_AS_STRING for avg.getCount() == 0, so we cannot get the exact same value back
        if (avg.getCount() != 0) {
            assertEquals(avg.getValueAsString(), parsed.getValueAsString());
        }
    }

    public void testReduceWithScriptedMetric() {
        String name = "test_scripted_metric";
        DocValueFormat formatter = randomNumericDocValueFormat();
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add regular InternalAvg
        aggregations.add(new InternalAvg(name, 50.0, 10L, formatter, null));

        // Add ScriptedMetric with ScriptedAvg object
        InternalScriptedMetric scriptedMetric1 = mock(InternalScriptedMetric.class);
        when(scriptedMetric1.getName()).thenReturn(name);
        List<Object> aggList = new ArrayList<>();
        aggList.add(new ScriptedAvg(100.0, 20L));
        when(scriptedMetric1.aggregationsList()).thenReturn(aggList);
        aggregations.add(scriptedMetric1);

        InternalAvg avg = new InternalAvg(name, 0.0, 0L, formatter, null);
        InternalAvg reduced = avg.reduce(aggregations, null);

        // Expected values:
        // From InternalAvg: sum=50.0, count=10
        // From scriptedMetric1: sum=100.0, count=20
        // Total: sum=150.0, count=30
        assertEquals(30L, reduced.getCount());
        assertEquals(150.0, reduced.getSum(), 0.0000001);
        assertEquals(5.0, reduced.getValue(), 0.0000001);  // 150/30
    }

    public void testReduceWithInternalAvgAggregation() {
        String name = "test_avg";
        DocValueFormat formatter = randomNumericDocValueFormat();
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add multiple InternalAvg aggregations
        aggregations.add(new InternalAvg(name, 50.0, 10L, formatter, null));
        aggregations.add(new InternalAvg(name, 100.0, 20L, formatter, null));
        aggregations.add(new InternalAvg(name, 150.0, 30L, formatter, null));

        InternalAvg avg = new InternalAvg(name, 0.0, 0L, formatter, null);
        InternalAvg reduced = avg.reduce(aggregations, null);

        // Expected values:
        // sum = 50.0 + 100.0 + 150.0 = 300.0
        // count = 10 + 20 + 30 = 60
        assertEquals(60L, reduced.getCount());
        assertEquals(300.0, reduced.getSum(), 0.0000001);
        assertEquals(5.0, reduced.getValue(), 0.0000001);  // 300/60
    }

    public void testReduceWithScriptedMetricInvalidType() {
        String name = "test_scripted_metric";
        DocValueFormat formatter = randomNumericDocValueFormat();
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add regular InternalAvg
        aggregations.add(new InternalAvg(name, 50.0, 10L, formatter, null));

        // Add ScriptedMetric with invalid return type (String instead of double[])
        InternalScriptedMetric scriptedMetric1 = mock(InternalScriptedMetric.class);
        when(scriptedMetric1.getName()).thenReturn(name);
        List<Object> aggList = new ArrayList<>();
        aggList.add("invalid_type");
        when(scriptedMetric1.aggregationsList()).thenReturn(aggList);
        aggregations.add(scriptedMetric1);

        InternalAvg avg = new InternalAvg(name, 0.0, 0L, formatter, null);

        // Expect an IllegalArgumentException when reducing with invalid type
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> avg.reduce(aggregations, null));
        assertEquals(
            "Invalid ScriptedMetric result for [test_scripted_metric] avg aggregation. Expected ScriptedAvg but received [java.lang.String]",
            e.getMessage()
        );
    }

    public void testReduceWithScriptedMetricInvalidArrayLength() {
        String name = "test_scripted_metric";
        DocValueFormat formatter = randomNumericDocValueFormat();
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add regular InternalAvg
        aggregations.add(new InternalAvg(name, 50.0, 10L, formatter, null));

        // Add ScriptedMetric with double array of wrong length (should be 2)
        InternalScriptedMetric scriptedMetric = mock(InternalScriptedMetric.class);
        when(scriptedMetric.getName()).thenReturn(name);
        List<Object> aggList = new ArrayList<>();
        aggList.add(new double[] { 100.0, 20.0, 30.0 });  // Add double array to list
        when(scriptedMetric.aggregationsList()).thenReturn(aggList);
        aggregations.add(scriptedMetric);

        InternalAvg avg = new InternalAvg(name, 0.0, 0L, formatter, null);

        // Expect an IllegalArgumentException when reducing with invalid array length
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> avg.reduce(aggregations, null));
        assertEquals(
            "Invalid ScriptedMetric result for [test_scripted_metric] avg aggregation. Expected ScriptedAvg but received [[D]",
            e.getMessage()
        );
    }

    @Override
    protected InternalAvg mutateInstance(InternalAvg instance) {
        String name = instance.getName();
        double sum = instance.getSum();
        long count = instance.getCount();
        DocValueFormat formatter = instance.getFormatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(sum)) {
                    sum += between(1, 100);
                } else {
                    sum = between(1, 100);
                }
                break;
            case 2:
                if (Double.isFinite(count)) {
                    count += between(1, 100);
                } else {
                    count = between(1, 100);
                }
                break;
            case 3:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalAvg(name, sum, count, formatter, metadata);
    }
}

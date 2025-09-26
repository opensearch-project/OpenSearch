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

import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalValueCountTests extends InternalAggregationTestCase<InternalValueCount> {

    @Override
    protected InternalValueCount createTestInstance(String name, Map<String, Object> metadata) {
        return new InternalValueCount(name, randomIntBetween(0, 100), metadata);
    }

    @Override
    protected void assertReduced(InternalValueCount reduced, List<InternalValueCount> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalValueCount::getValue).sum(), reduced.getValue(), 0);
    }

    @Override
    protected void assertFromXContent(InternalValueCount valueCount, ParsedAggregation parsedAggregation) {
        assertEquals(valueCount.getValue(), ((ParsedValueCount) parsedAggregation).getValue(), 0);
        assertEquals(valueCount.getValueAsString(), ((ParsedValueCount) parsedAggregation).getValueAsString());
    }

    public void testReduceWithScriptedMetric() {
        String name = "test_scripted_metric";
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add regular InternalValueCount
        aggregations.add(new InternalValueCount(name, 50L, null));

        // Add ScriptedMetric with Long value
        InternalScriptedMetric scriptedMetric1 = mock(InternalScriptedMetric.class);
        List<Object> aggList1 = new ArrayList<>();
        aggList1.add(20L);
        when(scriptedMetric1.aggregationsList()).thenReturn(aggList1);
        aggregations.add(scriptedMetric1);

        // Add ScriptedMetric with Integer value
        InternalScriptedMetric scriptedMetric2 = mock(InternalScriptedMetric.class);
        List<Object> aggList2 = new ArrayList<>();
        aggList2.add(30);
        when(scriptedMetric2.aggregationsList()).thenReturn(aggList2);
        aggregations.add(scriptedMetric2);

        // Add ScriptedMetric with Double value
        InternalScriptedMetric scriptedMetric3 = mock(InternalScriptedMetric.class);
        List<Object> aggList3 = new ArrayList<>();
        aggList3.add(10.5);
        when(scriptedMetric3.aggregationsList()).thenReturn(aggList3);
        aggregations.add(scriptedMetric3);

        InternalValueCount valueCount = new InternalValueCount(name, 0L, null);
        InternalValueCount reduced = (InternalValueCount) valueCount.reduce(aggregations, null);

        // Expected: 50 + 20 + 30 + 10 = 110
        assertEquals(110L, reduced.getValue());
    }

    public void testReduceWithInternalValueCountOnly() {
        // This test remains unchanged as it doesn't use ScriptedMetric
        String name = "test_value_count";
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add multiple InternalValueCount aggregations
        aggregations.add(new InternalValueCount(name, 50L, null));
        aggregations.add(new InternalValueCount(name, 30L, null));
        aggregations.add(new InternalValueCount(name, 20L, null));

        InternalValueCount valueCount = new InternalValueCount(name, 0L, null);
        InternalValueCount reduced = (InternalValueCount) valueCount.reduce(aggregations, null);

        // Expected: 50 + 30 + 20 = 100
        assertEquals(100L, reduced.getValue());
    }

    public void testReduceWithScriptedMetricInvalidValue() {
        String name = "test_scripted_metric";
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add regular InternalValueCount
        aggregations.add(new InternalValueCount(name, 50L, null));

        // Add ScriptedMetric with invalid value type (String instead of Number)
        InternalScriptedMetric scriptedMetric = mock(InternalScriptedMetric.class);
        List<Object> aggList = new ArrayList<>();
        aggList.add("invalid_value");
        when(scriptedMetric.aggregationsList()).thenReturn(aggList);
        aggregations.add(scriptedMetric);

        InternalValueCount valueCount = new InternalValueCount(name, 0L, null);

        // Expect an IllegalArgumentException when reducing with invalid value type
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> valueCount.reduce(aggregations, null));

        assertEquals(
            "Invalid ScriptedMetric result for ["
                + name
                + "] valueCount aggregation. "
                + "Expected numeric value from ScriptedMetric aggregation but got [java.lang.String]",
            e.getMessage()
        );
    }

    public void testReduceWithMultipleValuesInList() {
        String name = "test_scripted_metric";
        List<InternalAggregation> aggregations = new ArrayList<>();

        // Add regular InternalValueCount
        aggregations.add(new InternalValueCount(name, 50L, null));

        // Add ScriptedMetric with multiple values in the list
        InternalScriptedMetric scriptedMetric = mock(InternalScriptedMetric.class);
        List<Object> aggList = new ArrayList<>();
        aggList.add(20L);
        aggList.add(30);
        aggList.add(10.5);
        when(scriptedMetric.aggregationsList()).thenReturn(aggList);
        aggregations.add(scriptedMetric);

        InternalValueCount valueCount = new InternalValueCount(name, 0L, null);
        InternalValueCount reduced = (InternalValueCount) valueCount.reduce(aggregations, null);

        // Expected: 50 + 20 + 30 + 10 = 110
        assertEquals(110L, reduced.getValue());
    }

    @Override
    protected InternalValueCount mutateInstance(InternalValueCount instance) {
        String name = instance.getName();
        long value = instance.getValue();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(value)) {
                    value += between(1, 100);
                } else {
                    value = between(1, 100);
                }
                break;
            case 2:
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
        return new InternalValueCount(name, value, metadata);
    }
}

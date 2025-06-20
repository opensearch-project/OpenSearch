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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalTDigestPercentilesTests extends InternalPercentilesTestCase<InternalTDigestPercentiles> {

    @Override
    protected InternalTDigestPercentiles createTestInstance(
        String name,
        Map<String, Object> metadata,
        boolean keyed,
        DocValueFormat format,
        double[] percents,
        double[] values
    ) {
        final TDigestState state = new TDigestState(100);
        Arrays.stream(values).forEach(state::add);

        // the number of centroids is defined as <= the number of samples inserted
        assertTrue(state.centroidCount() <= values.length);
        return new InternalTDigestPercentiles(name, percents, state, keyed, format, metadata);
    }

    @Override
    protected void assertReduced(InternalTDigestPercentiles reduced, List<InternalTDigestPercentiles> inputs) {
        final TDigestState expectedState = new TDigestState(reduced.state.compression());

        long totalCount = 0;
        for (InternalTDigestPercentiles input : inputs) {
            assertArrayEquals(reduced.keys, input.keys, 0d);
            expectedState.add(input.state);
            totalCount += input.state.size();
        }

        assertEquals(totalCount, reduced.state.size());
        if (totalCount > 0) {
            assertEquals(expectedState.quantile(0), reduced.state.quantile(0), 0d);
            assertEquals(expectedState.quantile(1), reduced.state.quantile(1), 0d);
        }
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedTDigestPercentiles.class;
    }

    @Override
    protected InternalTDigestPercentiles mutateInstance(InternalTDigestPercentiles instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        TDigestState state = instance.state;
        boolean keyed = instance.keyed;
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 4)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                percents = Arrays.copyOf(percents, percents.length + 1);
                percents[percents.length - 1] = randomDouble() * 100;
                Arrays.sort(percents);
                break;
            case 2:
                TDigestState newState = new TDigestState(state.compression());
                newState.add(state);
                for (int i = 0; i < between(10, 100); i++) {
                    newState.add(randomDouble());
                }
                state = newState;
                break;
            case 3:
                keyed = keyed == false;
                break;
            case 4:
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
        return new InternalTDigestPercentiles(name, percents, state, keyed, formatter, metadata);
    }
}

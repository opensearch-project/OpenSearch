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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FieldMemoryStatsTests extends OpenSearchTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats stats = randomFieldMemoryStats();
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldMemoryStats read = new FieldMemoryStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats, read);
    }

    public void testHashCodeEquals() {
        FieldMemoryStats stats = randomFieldMemoryStats();
        assertEquals(stats, stats);
        assertEquals(stats.hashCode(), stats.hashCode());
        final Map<String, Long> map1 = new HashMap<>();
        map1.put("bar", 1L);
        FieldMemoryStats stats1 = new FieldMemoryStats(map1);
        final Map<String, Long> map2 = new HashMap<>();
        map2.put("foo", 2L);
        FieldMemoryStats stats2 = new FieldMemoryStats(map2);

        final Map<String, Long> map3 = new HashMap<>();
        map3.put("foo", 2L);
        map3.put("bar", 1L);
        FieldMemoryStats stats3 = new FieldMemoryStats(map3);

        final Map<String, Long> map4 = new HashMap<>();
        map4.put("foo", 2L);
        map4.put("bar", 1L);
        FieldMemoryStats stats4 = new FieldMemoryStats(map4);

        assertNotEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertNotEquals(stats2, stats3);
        assertEquals(stats4, stats3);

        stats1.add(stats2);
        assertEquals(stats1, stats3);
        assertEquals(stats1, stats4);
        assertEquals(stats1.hashCode(), stats3.hashCode());
    }

    public void testAdd() {
        final Map<String, Long> map1 = new HashMap<>();
        map1.put("bar", 1L);
        FieldMemoryStats stats1 = new FieldMemoryStats(map1);
        final Map<String, Long> map2 = new HashMap<>();
        map2.put("foo", 2L);
        FieldMemoryStats stats2 = new FieldMemoryStats(map2);

        final Map<String, Long> map3 = new HashMap<>();
        map3.put("bar", 1L);
        FieldMemoryStats stats3 = new FieldMemoryStats(map3);
        stats3.add(stats1);

        final Map<String, Long> map4 = new HashMap<>();
        map4.put("foo", 2L);
        map4.put("bar", 2L);
        FieldMemoryStats stats4 = new FieldMemoryStats(map4);
        assertNotEquals(stats3, stats4);
        stats3.add(stats2);
        assertEquals(stats3, stats4);
    }

    public static FieldMemoryStats randomFieldMemoryStats() {
        final Map<String, Long> map = new HashMap<>();
        int keys = randomIntBetween(1, 1000);
        for (int i = 0; i < keys; i++) {
            map.put(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), randomNonNegativeLong());
        }
        return new FieldMemoryStats(map);
    }
}

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

package org.opensearch.index.fielddata;

import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.FieldMemoryStatsTests;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FieldDataStatsTests extends OpenSearchTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats map = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        FieldDataStats stats = new FieldDataStats.Builder().memorySize(randomNonNegativeLong())
            .evictions(randomNonNegativeLong())
            .fieldMemoryStats(map)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldDataStats read = new FieldDataStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getEvictions(), read.getEvictions());
        assertEquals(stats.getMemorySize(), read.getMemorySize());
        assertEquals(stats.getFields(), read.getFields());
    }

    public void testClampingNegativeValues() throws IOException {
        Map<String, Long> fieldMap = new HashMap<>();
        fieldMap.put("field1", -100L);
        fieldMap.put("field2", 200L);
        fieldMap.put("field3", -50L);
        FieldMemoryStats fieldStats = new FieldMemoryStats(fieldMap);

        FieldDataStats stats = new FieldDataStats.Builder().memorySize(-500L).evictions(-10L).fieldMemoryStats(fieldStats).build();

        assertEquals(0L, stats.getMemorySizeInBytes());
        assertEquals(0L, stats.getEvictions());
        assertEquals(0L, stats.getFields().get("field1"));
        assertEquals(200L, stats.getFields().get("field2"));
        assertEquals(0L, stats.getFields().get("field3"));
    }
}

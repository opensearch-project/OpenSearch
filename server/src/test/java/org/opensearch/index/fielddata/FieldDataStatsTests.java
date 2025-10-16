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

import org.opensearch.common.FieldCountStats;
import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.FieldMemoryStatsTests;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class FieldDataStatsTests extends OpenSearchTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats memoryStats = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        FieldCountStats countStats = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldCountStats();
        // test both ctors
        FieldDataStats stats = randomBoolean()
            ? new FieldDataStats(randomNonNegativeLong(), randomNonNegativeLong(), memoryStats)
            : new FieldDataStats(randomNonNegativeLong(), randomNonNegativeLong(), memoryStats, randomNonNegativeLong(), countStats);
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldDataStats read = new FieldDataStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getEvictions(), read.getEvictions());
        assertEquals(stats.getMemorySize(), read.getMemorySize());
        assertEquals(stats.getFields(), read.getFields());
        assertEquals(stats.getItemCount(), read.getItemCount());
        assertEquals(stats.getFieldItemCounts(), read.getFieldItemCounts());
    }
}

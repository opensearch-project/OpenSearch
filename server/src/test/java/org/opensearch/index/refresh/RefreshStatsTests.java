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

package org.opensearch.index.refresh;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class RefreshStatsTests extends OpenSearchTestCase {

    public void testSerialize() throws IOException {
        RefreshStats stats = new RefreshStats.Builder().total(randomNonNegativeLong())
            .totalTimeInMillis(randomNonNegativeLong())
            .externalTotal(randomNonNegativeLong())
            .externalTotalTimeInMillis(randomNonNegativeLong())
            .listeners(between(0, Integer.MAX_VALUE))
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        RefreshStats read = new RefreshStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getTotal(), read.getTotal());
        assertEquals(stats.getExternalTotal(), read.getExternalTotal());
        assertEquals(stats.getListeners(), read.getListeners());
        assertEquals(stats.getTotalTimeInMillis(), read.getTotalTimeInMillis());
        assertEquals(stats.getExternalTotalTimeInMillis(), read.getExternalTotalTimeInMillis());
    }
}

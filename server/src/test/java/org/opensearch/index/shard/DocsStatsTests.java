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

package org.opensearch.index.shard;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DocsStatsTests extends OpenSearchTestCase {

    public void testCalculateAverageDocSize() throws Exception {
        DocsStats stats = new DocsStats.Builder().count(10).deleted(2).totalSizeInBytes(120).build();
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));

        stats.add(new DocsStats.Builder().count(0).deleted(0).totalSizeInBytes(0).build());
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));

        stats.add(new DocsStats.Builder().count(8).deleted(30).totalSizeInBytes(480).build());
        assertThat(stats.getCount(), equalTo(18L));
        assertThat(stats.getDeleted(), equalTo(32L));
        assertThat(stats.getTotalSizeInBytes(), equalTo(600L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(12L));
    }

    public void testUninitialisedShards() {
        DocsStats stats = new DocsStats.Builder().count(0).deleted(0).totalSizeInBytes(-1).build();
        assertThat(stats.getTotalSizeInBytes(), equalTo(-1L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(0L));
        stats.add(new DocsStats.Builder().count(0).deleted(0).totalSizeInBytes(-1).build());
        assertThat(stats.getTotalSizeInBytes(), equalTo(-1L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(0L));
        stats.add(new DocsStats.Builder().count(1).deleted(0).totalSizeInBytes(10).build());
        assertThat(stats.getTotalSizeInBytes(), equalTo(10L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));
        stats.add(new DocsStats.Builder().count(0).deleted(0).totalSizeInBytes(-1).build());
        assertThat(stats.getTotalSizeInBytes(), equalTo(10L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(10L));
        stats.add(new DocsStats.Builder().count(1).deleted(0).totalSizeInBytes(20).build());
        assertThat(stats.getTotalSizeInBytes(), equalTo(30L));
        assertThat(stats.getAverageSizeInBytes(), equalTo(15L));
    }

    public void testSerialize() throws Exception {
        DocsStats originalStats = new DocsStats.Builder().count(randomNonNegativeLong())
            .deleted(randomNonNegativeLong())
            .totalSizeInBytes(randomNonNegativeLong())
            .build();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                DocsStats cloneStats = new DocsStats(in);
                assertThat(cloneStats.getCount(), equalTo(originalStats.getCount()));
                assertThat(cloneStats.getDeleted(), equalTo(originalStats.getDeleted()));
                assertThat(cloneStats.getAverageSizeInBytes(), equalTo(originalStats.getAverageSizeInBytes()));
            }
        }
    }
}

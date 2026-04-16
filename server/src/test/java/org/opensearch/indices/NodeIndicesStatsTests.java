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

package org.opensearch.indices;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.DocStatusStats;
import org.opensearch.action.admin.indices.stats.SearchResponseStatusStats;
import org.opensearch.action.admin.indices.stats.StatusCounterStats;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.rest.StatusType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.LongAdder;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;

public class NodeIndicesStatsTests extends OpenSearchTestCase {

    public void testInvalidLevel() {
        CommonStats oldStats = new CommonStats();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats requestStats = new SearchRequestStats(clusterSettings);
        StatusCounterStats statusCounterStats = new StatusCounterStats();
        final NodeIndicesStats stats = new NodeIndicesStats(oldStats, Collections.emptyMap(), requestStats, statusCounterStats);
        final String level = randomAlphaOfLength(16);
        final ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("level", level));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stats.toXContent(null, params));
        assertThat(
            e,
            hasToString(containsString("level parameter must be one of [indices] or [node] or [shards] but was [" + level + "]"))
        );
    }

    public void testSerializationForStatusCounterStats() throws IOException {
        StatusCounterStats stats = createStatusCounters();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                StatusCounterStats deserializedStats = new StatusCounterStats(in);

                if (stats.getDocStatusStats() == null) {
                    assertNull(deserializedStats.getDocStatusStats());
                    return;
                }

                DocStatusStats docStats = stats.getDocStatusStats();
                DocStatusStats deserializedDocStats = deserializedStats.getDocStatusStats();

                assertTrue(
                    Arrays.equals(
                        docStats.getDocStatusCounter(),
                        deserializedDocStats.getDocStatusCounter(),
                        Comparator.comparingLong(LongAdder::longValue)
                    )
                );

                if (stats.getSearchResponseStatusStats() == null) {
                    assertNull(deserializedStats.getSearchResponseStatusStats());
                    return;
                }

                SearchResponseStatusStats searchStats = stats.getSearchResponseStatusStats();
                SearchResponseStatusStats deserializedSearchStats = deserializedStats.getSearchResponseStatusStats();

                assertTrue(
                    Arrays.equals(
                        searchStats.getSearchResponseStatusCounter(),
                        deserializedSearchStats.getSearchResponseStatusCounter(),
                        Comparator.comparingLong(LongAdder::longValue)
                    )
                );
            }
        }
    }

    public void testToXContentForStatusCounterStats() throws IOException {
        StatusCounterStats statusCounterStats = createStatusCounters();
        LongAdder[] docStatusCounter = statusCounterStats.getDocStatusStats().getDocStatusCounter();
        LongAdder[] searchResponseStatusCounter = statusCounterStats.getSearchResponseStatusStats().getSearchResponseStatusCounter();

        long docStatusSuccesses = docStatusCounter[0].longValue() + docStatusCounter[1].longValue() + docStatusCounter[2].longValue();
        long searchResponseStatusSuccesses = searchResponseStatusCounter[0].longValue() + searchResponseStatusCounter[1].longValue()
            + searchResponseStatusCounter[2].longValue();

        String expected = "{\"status_counter\":{\"doc_status\":{\""
            + StatusType.SUCCESS
            + "\":"
            + docStatusSuccesses
            + ",\""
            + StatusType.USER_ERROR
            + "\":"
            + docStatusCounter[3].longValue()
            + ",\""
            + StatusType.SYSTEM_FAILURE
            + "\":"
            + docStatusCounter[4].longValue()
            + "},\"search_response_status\":{\""
            + StatusType.SUCCESS
            + "\":"
            + searchResponseStatusSuccesses
            + ",\""
            + StatusType.USER_ERROR
            + "\":"
            + searchResponseStatusCounter[3].longValue()
            + ",\""
            + StatusType.SYSTEM_FAILURE
            + "\":"
            + searchResponseStatusCounter[4].longValue()
            + "}}}";

        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        xContentBuilder.startObject();
        xContentBuilder = statusCounterStats.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        assertEquals(expected, xContentBuilder.toString());
    }

    private StatusCounterStats createStatusCounters() {
        StatusCounterStats statusCounters = new StatusCounterStats();

        for (int i = 1; i < 6; ++i) {
            statusCounters.getDocStatusStats().add(RestStatus.fromCode(i * 100), randomLongBetween(0, 100));
            statusCounters.getSearchResponseStatusStats().add(RestStatus.fromCode(i * 100), randomLongBetween(0, 100));
        }

        return statusCounters;
    }

}

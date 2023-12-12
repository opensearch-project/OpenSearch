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
import org.opensearch.action.search.SearchRequestListenerManager;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;

public class NodeIndicesStatsTests extends OpenSearchTestCase {

    public void testInvalidLevel() {
        CommonStats oldStats = new CommonStats();
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestListenerManager listenerManager = new SearchRequestListenerManager(clusterService);
        SearchRequestStats requestStats = new SearchRequestStats(clusterService, listenerManager);
        final NodeIndicesStats stats = new NodeIndicesStats(oldStats, Collections.emptyMap(), requestStats);
        final String level = randomAlphaOfLength(16);
        final ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("level", level));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stats.toXContent(null, params));
        assertThat(
            e,
            hasToString(containsString("level parameter must be one of [indices] or [node] or [shards] but was [" + level + "]"))
        );
    }

}

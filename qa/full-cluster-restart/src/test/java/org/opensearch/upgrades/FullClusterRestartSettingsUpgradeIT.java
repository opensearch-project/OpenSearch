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

package org.opensearch.upgrades;

import org.opensearch.LegacyESVersion;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.SniffConnectionStrategy;

import java.io.IOException;
import java.util.Collections;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.transport.RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.opensearch.transport.SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS;
import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartSettingsUpgradeIT extends AbstractFullClusterRestartTestCase {

    public void testRemoteClusterSettingsUpgraded() throws IOException {
        assumeTrue("skip_unavailable did not exist until 6.1.0", getOldClusterVersion().onOrAfter(LegacyESVersion.V_6_1_0));
        assumeTrue("settings automatically upgraded since 6.5.0", getOldClusterVersion().before(LegacyESVersion.V_6_5_0));
        if (isRunningAgainstOldCluster()) {
            final Request putSettingsRequest = new Request("PUT", "/_cluster/settings");
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.startObject("persistent");
                    {
                        builder.field("search.remote.foo.skip_unavailable", true);
                        builder.field("search.remote.foo.seeds", Collections.singletonList("localhost:9200"));
                    }
                    builder.endObject();
                }
                builder.endObject();
                putSettingsRequest.setJsonEntity(Strings.toString(builder));
            }
            client().performRequest(putSettingsRequest);

            final Request getSettingsRequest = new Request("GET", "/_cluster/settings");
            final Response response = client().performRequest(getSettingsRequest);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, response.getEntity().getContent())) {
                final ClusterGetSettingsResponse clusterGetSettingsResponse = ClusterGetSettingsResponse.fromXContent(parser);
                final Settings settings = clusterGetSettingsResponse.getPersistentSettings();

                assertTrue(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").get(settings));
                assertTrue(SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                assertThat(
                        SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").get(settings),
                        equalTo(Collections.singletonList("localhost:9200")));
            }

            assertSettingDeprecationsAndWarnings(new Setting<?>[]{
                    SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo"),
                    SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo")});
        } else {
            final Request getSettingsRequest = new Request("GET", "/_cluster/settings");
            final Response getSettingsResponse = client().performRequest(getSettingsRequest);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, getSettingsResponse.getEntity().getContent())) {
                final ClusterGetSettingsResponse clusterGetSettingsResponse = ClusterGetSettingsResponse.fromXContent(parser);
                final Settings settings = clusterGetSettingsResponse.getPersistentSettings();

                assertFalse(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(
                        settings.toString(),
                        RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").get(settings));
                assertFalse(SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                assertTrue(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                assertThat(
                        SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").get(settings),
                        equalTo(Collections.singletonList("localhost:9200")));
            }
        }
    }

}

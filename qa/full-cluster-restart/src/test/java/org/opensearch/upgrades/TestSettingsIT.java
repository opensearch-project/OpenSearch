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

import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.transport.RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.opensearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS;
import static org.opensearch.transport.SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY;

public class TestSettingsIT extends AbstractFullClusterRestartTestCase {

        public void testClusterSetting() throws IOException {
            if (isRunningAgainstOldCluster()) {
                final Request putSettingsRequest = new Request("PUT", "/_cluster/settings");
                try (XContentBuilder builder = jsonBuilder()) {
                    builder.startObject();
                    {
                        builder.startObject("persistent");
                        {
                            builder.field("search.remote.foo.skip_unavailable", true);
                            builder.field("search.remote.foo.seeds", Collections.singletonList("localhost:9200"));
                            builder.field("search.remote.foo.proxy", "localhost:9200");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    putSettingsRequest.setJsonEntity(Strings.toString(builder));
                }
                client().performRequest(putSettingsRequest);
            }
            else{
                Request getSettingsRequest = new Request("GET", "/_cluster/settings");
                Response res = client().performRequest(getSettingsRequest);
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, res.getEntity().getContent())) {
                    final ClusterGetSettingsResponse clusterGetSettingsResponse = ClusterGetSettingsResponse.fromXContent(parser);
                    final Settings settings = clusterGetSettingsResponse.getPersistentSettings();
                    assertTrue(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
                    assertTrue(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
                    assertTrue(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace("foo").exists(settings));
                    
                    assertTrue(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").get(settings));
                    assertEquals(String.valueOf(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").get(settings)), 
                                "[localhost:9200]");
                    assertEquals(String.valueOf(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace("foo").get(settings)), 
                                "localhost:9200");
                }
            }

        }   
}

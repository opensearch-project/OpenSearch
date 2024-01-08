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

package org.opensearch.action.get;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.common.UUIDs.randomBase64UUID;

public class TransportGetActionTests extends OpenSearchTestCase {

    private static ClusterState clusterState(ReplicationType replicationType) {
        final Index index1 = new Index("index1", randomBase64UUID());
        return ClusterState.builder(new ClusterName(TransportGetActionTests.class.getSimpleName()))
            .metadata(
                new Metadata.Builder().put(
                    new IndexMetadata.Builder(index1.getName()).settings(
                        Settings.builder()
                            .put("index.version.created", Version.CURRENT)
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
                            .put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType)
                    )
                )
            )
            .build();
    }

    public void testShouldForcePrimaryRouting() {

        Metadata metadata = clusterState(ReplicationType.SEGMENT).getMetadata();

        // should return false since preference is set for request
        assertFalse(TransportGetAction.shouldForcePrimaryRouting(metadata, true, Preference.REPLICA.type(), "index1"));

        // should return false since request is not realtime
        assertFalse(TransportGetAction.shouldForcePrimaryRouting(metadata, false, null, "index1"));

        // should return true since segment replication is enabled
        assertTrue(TransportGetAction.shouldForcePrimaryRouting(metadata, true, null, "index1"));

        // should return false since index doesn't exist
        assertFalse(TransportGetAction.shouldForcePrimaryRouting(metadata, true, null, "index3"));

        metadata = clusterState(ReplicationType.DOCUMENT).getMetadata();

        // should fail since document replication enabled
        assertFalse(TransportGetAction.shouldForcePrimaryRouting(metadata, true, null, "index1"));

    }

}

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

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;

public class SnapshotUtilsTests extends OpenSearchTestCase {
    public void testIndexNameFiltering() {
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] {}, new String[] { "foo", "bar", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "*" }, new String[] { "foo", "bar", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "_all" }, new String[] { "foo", "bar", "baz" });
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "foo", "bar", "baz" },
            new String[] { "foo", "bar", "baz" }
        );
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "foo" }, new String[] { "foo" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "baz", "not_available" }, new String[] { "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "ba*", "-bar", "-baz" }, new String[] {});
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "-bar" }, new String[] { "foo", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "-ba*" }, new String[] { "foo" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "+ba*" }, new String[] { "bar", "baz" });
        assertIndexNameFiltering(new String[] { "foo", "bar", "baz" }, new String[] { "+bar", "+foo" }, new String[] { "bar", "foo" });
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "zzz", "bar" },
            IndicesOptions.lenientExpandOpen(),
            new String[] { "bar" }
        );
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "" },
            IndicesOptions.lenientExpandOpen(),
            new String[] {}
        );
        assertIndexNameFiltering(
            new String[] { "foo", "bar", "baz" },
            new String[] { "foo", "", "ba*" },
            IndicesOptions.lenientExpandOpen(),
            new String[] { "foo", "bar", "baz" }
        );
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, String[] expected) {
        assertIndexNameFiltering(indices, filter, IndicesOptions.lenientExpandOpen(), expected);
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, IndicesOptions indicesOptions, String[] expected) {
        List<String> indicesList = Arrays.asList(indices);
        List<String> actual = SnapshotUtils.filterIndices(indicesList, filter, indicesOptions);
        assertThat(actual, containsInAnyOrder(expected));
    }

    public void testValidateSnapshotsBackingAnyIndex() {
        final String repoName = "test-repo";
        final SnapshotId snapshotId1 = new SnapshotId("testSnapshot1", "uuid1");
        final SnapshotId snapshotId2 = new SnapshotId("testSnapshot2", "uuid2");
        ClusterState clusterState = getClusterState(snapshotId1, repoName);
        try {
            SnapshotUtils.validateSnapshotsBackingAnyIndex(clusterState, List.of(snapshotId2), repoName);
        } catch (Exception e) {
            fail("Should not have thrown any exception");
        }
    }

    public void testValidateSnapshotsBackingAnyIndexThrowsException() {
        final String repoName = "test-repo";
        final SnapshotId snapshotId1 = new SnapshotId("testSnapshot1", "uuid1");
        ClusterState clusterState = getClusterState(snapshotId1, repoName);
        expectThrows(
            SnapshotDeletionException.class,
            () -> SnapshotUtils.validateSnapshotsBackingAnyIndex(clusterState, List.of(snapshotId1), repoName)
        );
    }

    private static ClusterState getClusterState(SnapshotId snapshotId, String repoName) {
        final String index = "test-index";
        Snapshot snapshot = new Snapshot(repoName, snapshotId);
        final Metadata.Builder metaBuilder = Metadata.builder(Metadata.EMPTY_METADATA);
        metaBuilder.put(
            IndexMetadata.builder(index)
                .settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT.id)
                        .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                        .put(IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), snapshot.getRepository())
                        .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.getKey(), snapshot.getSnapshotId().getUUID())
                        .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.getKey(), snapshot.getSnapshotId().getName())
                )
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(),
            false
        );
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metaBuilder).build();
    }
}

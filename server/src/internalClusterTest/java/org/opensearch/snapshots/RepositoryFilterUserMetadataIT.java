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

import org.apache.lucene.index.IndexCommit;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;

public class RepositoryFilterUserMetadataIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MetadataFilteringPlugin.class);
    }

    public void testFilteredRepoMetadataIsUsed() {
        final String clusterManagerName = internalCluster().getClusterManagerName();
        final String repoName = "test-repo";
        Settings.Builder settings = Settings.builder()
            .put("location", randomRepoPath())
            .put(MetadataFilteringPlugin.CLUSTER_MANAGER_SETTING_VALUE, clusterManagerName);
        createRepository(repoName, MetadataFilteringPlugin.TYPE, settings);
        createIndex("test-idx");
        final SnapshotInfo snapshotInfo = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "test-snap")
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(
            snapshotInfo.userMetadata(),
            is(Collections.singletonMap(MetadataFilteringPlugin.MOCK_FILTERED_META, clusterManagerName))
        );
    }

    // Mock plugin that stores the name of the cluster-manager node that started a snapshot in each snapshot's metadata
    public static final class MetadataFilteringPlugin extends org.opensearch.plugins.Plugin implements RepositoryPlugin {

        private static final String MOCK_FILTERED_META = "mock_filtered_meta";

        private static final String CLUSTER_MANAGER_SETTING_VALUE = "initial_cluster_manager";

        private static final String TYPE = "mock_meta_filtering";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                "mock_meta_filtering",
                metadata -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {

                    // Storing the initially expected metadata value here to verify that #filterUserMetadata is only called once on the
                    // initial cluster-manager node starting the snapshot
                    private final String initialMetaValue = metadata.settings().get(CLUSTER_MANAGER_SETTING_VALUE);

                    @Override
                    public void finalizeSnapshot(
                        ShardGenerations shardGenerations,
                        long repositoryStateId,
                        Metadata clusterMetadata,
                        SnapshotInfo snapshotInfo,
                        Version repositoryMetaVersion,
                        Function<ClusterState, ClusterState> stateTransformer,
                        ActionListener<RepositoryData> listener
                    ) {
                        super.finalizeSnapshot(
                            shardGenerations,
                            repositoryStateId,
                            clusterMetadata,
                            snapshotInfo,
                            repositoryMetaVersion,
                            stateTransformer,
                            listener
                        );
                    }

                    @Override
                    public void snapshotShard(
                        Store store,
                        MapperService mapperService,
                        SnapshotId snapshotId,
                        IndexId indexId,
                        IndexCommit snapshotIndexCommit,
                        String shardStateIdentifier,
                        IndexShardSnapshotStatus snapshotStatus,
                        Version repositoryMetaVersion,
                        Map<String, Object> userMetadata,
                        ActionListener<String> listener
                    ) {
                        assertThat(userMetadata, is(Collections.singletonMap(MOCK_FILTERED_META, initialMetaValue)));
                        super.snapshotShard(
                            store,
                            mapperService,
                            snapshotId,
                            indexId,
                            snapshotIndexCommit,
                            shardStateIdentifier,
                            snapshotStatus,
                            repositoryMetaVersion,
                            userMetadata,
                            listener
                        );
                    }

                    @Override
                    public Map<String, Object> adaptUserMetadata(Map<String, Object> userMetadata) {
                        return Collections.singletonMap(MOCK_FILTERED_META, clusterService.getNodeName());
                    }
                }
            );
        }
    }
}

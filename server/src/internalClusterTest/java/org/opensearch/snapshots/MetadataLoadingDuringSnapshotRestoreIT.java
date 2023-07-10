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

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.rest.RestStatus;
import org.opensearch.snapshots.mockstore.MockRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * This class tests whether global and index metadata are only loaded from the repository when needed.
*/
public class MetadataLoadingDuringSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        /// This test uses a snapshot/restore plugin implementation that
        // counts the number of times metadata are loaded
        return Collections.singletonList(CountingMockRepositoryPlugin.class);
    }

    public void testWhenMetadataAreLoaded() throws Exception {
        createIndex("docs");
        indexRandom(
            true,
            client().prepareIndex("docs").setId("1").setSource("rank", 1),
            client().prepareIndex("docs").setId("2").setSource("rank", 2),
            client().prepareIndex("docs").setId("3").setSource("rank", 3),
            client().prepareIndex("others").setSource("rank", 4),
            client().prepareIndex("others").setSource("rank", 5)
        );

        createRepository("repository", CountingMockRepositoryPlugin.TYPE);

        // Creating a snapshot does not load any metadata
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("repository", "snap")
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 0);
        assertIndexMetadataLoads("snap", "others", 0);

        // Getting a snapshot does not load any metadata
        GetSnapshotsResponse getSnapshotsResponse = client().admin()
            .cluster()
            .prepareGetSnapshots("repository")
            .addSnapshots("snap")
            .setVerbose(randomBoolean())
            .get();
        assertThat(getSnapshotsResponse.getSnapshots(), hasSize(1));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 0);
        assertIndexMetadataLoads("snap", "others", 0);

        // Getting the status of a snapshot loads indices metadata but not global metadata
        SnapshotsStatusResponse snapshotStatusResponse = client().admin()
            .cluster()
            .prepareSnapshotStatus("repository")
            .setSnapshots("snap")
            .get();
        assertThat(snapshotStatusResponse.getSnapshots(), hasSize(1));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 1);
        assertIndexMetadataLoads("snap", "others", 1);

        assertAcked(client().admin().indices().prepareDelete("docs", "others"));

        // Restoring a snapshot loads indices metadata but not the global state
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot("repository", "snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 2);
        assertIndexMetadataLoads("snap", "others", 2);

        assertAcked(client().admin().indices().prepareDelete("docs"));

        // Restoring a snapshot with selective indices loads only required index metadata
        restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot("repository", "snap")
            .setIndices("docs")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 0);
        assertIndexMetadataLoads("snap", "docs", 3);
        assertIndexMetadataLoads("snap", "others", 2);

        assertAcked(client().admin().indices().prepareDelete("docs", "others"));

        // Restoring a snapshot including the global state loads it with the index metadata
        restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot("repository", "snap")
            .setIndices("docs", "oth*")
            .setRestoreGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().status(), equalTo(RestStatus.OK));
        assertGlobalMetadataLoads("snap", 1);
        assertIndexMetadataLoads("snap", "docs", 4);
        assertIndexMetadataLoads("snap", "others", 3);

        // Deleting a snapshot does not load the global metadata state but loads each index metadata
        assertAcked(client().admin().cluster().prepareDeleteSnapshot("repository", "snap").get());
        assertGlobalMetadataLoads("snap", 1);
        assertIndexMetadataLoads("snap", "docs", 4);
        assertIndexMetadataLoads("snap", "others", 3);
    }

    private void assertGlobalMetadataLoads(final String snapshot, final int times) {
        AtomicInteger count = getCountingMockRepository().globalMetadata.get(snapshot);
        if (times == 0) {
            assertThat("Global metadata for " + snapshot + " must not have been loaded", count, nullValue());
        } else {
            assertThat("Global metadata for " + snapshot + " must have been loaded " + times + " times", count.get(), equalTo(times));
        }
    }

    private void assertIndexMetadataLoads(final String snapshot, final String index, final int times) {
        final String key = key(snapshot, index);
        AtomicInteger count = getCountingMockRepository().indicesMetadata.get(key);
        if (times == 0) {
            assertThat("Index metadata for " + key + " must not have been loaded", count, nullValue());
        } else {
            assertThat("Index metadata for " + key + " must have been loaded " + times + " times", count.get(), equalTo(times));
        }
    }

    private CountingMockRepository getCountingMockRepository() {
        String clusterManager = internalCluster().getClusterManagerName();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, clusterManager);
        Repository repository = repositoriesService.repository("repository");
        assertThat(repository, instanceOf(CountingMockRepository.class));
        return (CountingMockRepository) repository;
    }

    /** Compute a map key for the given snapshot and index names **/
    private static String key(final String snapshot, final String index) {
        return snapshot + ":" + index;
    }

    /** A mocked repository that counts the number of times global/index metadata are accessed **/
    public static class CountingMockRepository extends MockRepository {

        final Map<String, AtomicInteger> globalMetadata = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> indicesMetadata = new ConcurrentHashMap<>();

        public CountingMockRepository(
            final RepositoryMetadata metadata,
            final Environment environment,
            final NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            globalMetadata.computeIfAbsent(snapshotId.getName(), (s) -> new AtomicInteger(0)).incrementAndGet();
            return super.getSnapshotGlobalMetadata(snapshotId);
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId indexId)
            throws IOException {
            indicesMetadata.computeIfAbsent(key(snapshotId.getName(), indexId.getName()), (s) -> new AtomicInteger(0)).incrementAndGet();
            return super.getSnapshotIndexMetaData(PlainActionFuture.get(this::getRepositoryData), snapshotId, indexId);
        }
    }

    /** A plugin that uses CountingMockRepository as implementation of the Repository **/
    public static class CountingMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "countingmock";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new CountingMockRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }
}

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

package org.opensearch.repositories.blobstore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.index.snapshots.IndexShardSnapshotFailedException;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

/**
 * This class tests the behavior of {@link BlobStoreRepository} when it
 * restores a shard from a snapshot but some files with same names already
 * exist on disc.
 */
public class BlobStoreRepositoryRestoreTests extends IndexShardTestCase {

    /**
     * Restoring a snapshot that contains multiple files must succeed even when
     * some files already exist in the shard's store.
     */
    public void testRestoreSnapshotWithExistingFiles() throws IOException {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "_doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            snapshotShard(shard, snapshot, repository);

            // capture current store files
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();
            assertFalse(storeFiles.asMap().isEmpty());

            // close the shard
            closeShards(shard);

            // delete some random files in the store
            List<String> deletedFiles = randomSubsetOf(randomIntBetween(1, storeFiles.size() - 1), storeFiles.asMap().keySet());
            for (String deletedFile : deletedFiles) {
                Files.delete(shard.shardPath().resolveIndex().resolve(deletedFile));
            }

            // build a new shard using the same store directory as the closed shard
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
                shard.routingEntry(),
                RecoverySource.ExistingStoreRecoverySource.INSTANCE
            );
            shard = newShard(
                shardRouting,
                shard.shardPath(),
                shard.indexSettings().getIndexMetadata(),
                null,
                null,
                new InternalEngineFactory(),
                new EngineConfigFactory(shard.indexSettings()),
                () -> {},
                RetentionLeaseSyncer.EMPTY,
                EMPTY_EVENT_LISTENER,
                null
            );

            // restore the shard
            recoverShardFromSnapshot(shard, snapshot, repository);

            // check that the shard is not corrupted
            TestUtil.checkIndex(shard.store().directory());

            // check that all files have been restored
            final Directory directory = shard.store().directory();
            final List<String> directoryFiles = Arrays.asList(directory.listAll());

            for (StoreFileMetadata storeFile : storeFiles) {
                String fileName = storeFile.name();
                assertTrue("File [" + fileName + "] does not exist in store directory", directoryFiles.contains(fileName));
                assertEquals(storeFile.length(), shard.store().directory().fileLength(fileName));
            }
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false, false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    public void testSnapshotWithConflictingName() throws Exception {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "_doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            final String shardGen = snapshotShard(shard, snapshot, repository);
            assertNotNull(shardGen);
            final Snapshot snapshotWithSameName = new Snapshot(
                repository.getMetadata().name(),
                new SnapshotId(snapshot.getSnapshotId().getName(), "_uuid2")
            );
            final ShardGenerations shardGenerations = ShardGenerations.builder().put(indexId, 0, shardGen).build();
            PlainActionFuture.<RepositoryData, Exception>get(
                f -> repository.finalizeSnapshot(
                    shardGenerations,
                    RepositoryData.EMPTY_REPO_GEN,
                    Metadata.builder().put(shard.indexSettings().getIndexMetadata(), false).build(),
                    new SnapshotInfo(
                        snapshot.getSnapshotId(),
                        shardGenerations.indices().stream().map(IndexId::getName).collect(Collectors.toList()),
                        Collections.emptyList(),
                        0L,
                        null,
                        1L,
                        6,
                        Collections.emptyList(),
                        true,
                        Collections.emptyMap(),
                        false,
                        0
                    ),
                    Version.CURRENT,
                    Function.identity(),
                    Priority.NORMAL,
                    f
                )
            );
            IndexShardSnapshotFailedException isfe = expectThrows(
                IndexShardSnapshotFailedException.class,
                () -> snapshotShard(shard, snapshotWithSameName, repository)
            );
            assertThat(isfe.getMessage(), containsString("Duplicate snapshot name"));
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false, false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    /** Create a {@link Repository} with a random name **/
    private Repository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }

    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                .build()
        );
    }
}

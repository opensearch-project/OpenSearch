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

package org.opensearch.repositories;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.Collection;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class RepositoriesServiceIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockRepository.Plugin.class);
    }

    public void testUpdateRepository() {
        final InternalTestCluster cluster = internalCluster();

        final String repositoryName = "test-repo";

        final Client client = client();
        final RepositoriesService repositoriesService = cluster.getDataOrClusterManagerNodeInstances(RepositoriesService.class)
            .iterator()
            .next();

        final Settings.Builder repoSettings = Settings.builder().put("location", randomRepoPath());
        OpenSearchIntegTestCase.putRepositoryWithNoSettingOverrides(
            client().admin().cluster(),
            repositoryName,
            FsRepository.TYPE,
            true,
            repoSettings
        );

        final GetRepositoriesResponse originalGetRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(repositoryName)
            .get();

        assertThat(originalGetRepositoriesResponse.repositories(), hasSize(1));
        RepositoryMetadata originalRepositoryMetadata = originalGetRepositoriesResponse.repositories().get(0);

        assertThat(originalRepositoryMetadata.type(), equalTo(FsRepository.TYPE));

        final Repository originalRepository = repositoriesService.repository(repositoryName);
        assertThat(originalRepository, instanceOf(FsRepository.class));

        final boolean updated = randomBoolean();
        final String updatedRepositoryType = updated ? "mock" : FsRepository.TYPE;

        OpenSearchIntegTestCase.putRepositoryWithNoSettingOverrides(
            client().admin().cluster(),
            repositoryName,
            updatedRepositoryType,
            true,
            repoSettings
        );

        final GetRepositoriesResponse updatedGetRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(repositoryName)
            .get();

        assertThat(updatedGetRepositoriesResponse.repositories(), hasSize(1));
        final RepositoryMetadata updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);

        assertThat(updatedRepositoryMetadata.type(), equalTo(updatedRepositoryType));

        final Repository updatedRepository = repositoriesService.repository(repositoryName);
        assertThat(updatedRepository, updated ? not(sameInstance(originalRepository)) : sameInstance(originalRepository));
    }

    public void testSystemRepositoryCantBeCreated() {
        internalCluster();
        final String repositoryName = "test-repo";
        final Settings.Builder repoSettings = Settings.builder().put("system_repository", true).put("location", randomRepoPath());

        assertThrows(RepositoryException.class, () -> createRepository(repositoryName, FsRepository.TYPE, repoSettings));
    }

    public void testCreatSnapAndUpdateReposityCauseInfiniteLoop() throws InterruptedException {
        // create index
        internalCluster();
        String indexName = "test-index";
        createIndex(indexName, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 0).put(SETTING_NUMBER_OF_SHARDS, 1).build());
        index(indexName, "_doc", "1", Collections.singletonMap("user", generateRandomStringArray(1, 10, false, false)));
        flush(indexName);

        // create repository
        final String repositoryName = "test-repo";
        Settings.Builder repoSettings = Settings.builder()
            .put("location", randomRepoPath())
            .put("max_snapshot_bytes_per_sec", "10mb")
            .put("max_restore_bytes_per_sec", "10mb");
        OpenSearchIntegTestCase.putRepositoryWithNoSettingOverrides(
            client().admin().cluster(),
            repositoryName,
            FsRepository.TYPE,
            true,
            repoSettings
        );

        String snapshotName = "test-snapshot";
        Runnable createSnapshot = () -> {
            logger.info("--> begining snapshot");
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repositoryName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
            logger.info("--> finishing snapshot");
        };

        // snapshot mab be failed when updating repository
        Thread thread = new Thread(() -> {
            try {
                createSnapshot.run();
            } catch (Exception e) {
                assertThat(e, instanceOf(RepositoryException.class));
                assertThat(e, hasToString(containsString(("the repository has been changed, try again"))));
            }
        });
        thread.start();

        try {
            logger.info("--> begin to reset repository");
            repoSettings = Settings.builder().put("location", randomRepoPath()).put("max_snapshot_bytes_per_sec", "300mb");
            OpenSearchIntegTestCase.putRepositoryWithNoSettingOverrides(
                client().admin().cluster(),
                repositoryName,
                FsRepository.TYPE,
                true,
                repoSettings
            );
            logger.info("--> finish to reset repository");
        } catch (IllegalStateException e) {
            assertThat(e, hasToString(containsString(("trying to modify or unregister repository that is currently used"))));
        }

        // after updating repository, snapshot should be success
        createSnapshot.run();

        thread.join();
    }
}

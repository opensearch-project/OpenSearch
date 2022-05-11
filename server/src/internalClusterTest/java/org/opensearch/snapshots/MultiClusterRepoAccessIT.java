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

import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.Environment;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.NodeConfigurationSource;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.nio.MockNioTransportPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class MultiClusterRepoAccessIT extends AbstractSnapshotIntegTestCase {

    private InternalTestCluster secondCluster;
    private Path repoPath;

    @Before
    public void startSecondCluster() throws IOException, InterruptedException {
        repoPath = randomRepoPath();
        secondCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            0,
            0,
            "second_cluster",
            new NodeConfigurationSource() {
                @Override
                public Settings nodeSettings(int nodeOrdinal) {
                    return Settings.builder()
                        .put(MultiClusterRepoAccessIT.this.nodeSettings(nodeOrdinal))
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .put(Environment.PATH_REPO_SETTING.getKey(), repoPath)
                        .build();
                }

                @Override
                public Path nodeConfigPath(int nodeOrdinal) {
                    return null;
                }
            },
            0,
            "leader",
            Arrays.asList(
                OpenSearchIntegTestCase.TestSeedPlugin.class,
                MockHttpTransport.TestPlugin.class,
                MockTransportService.TestPlugin.class,
                MockNioTransportPlugin.class,
                InternalSettingsPlugin.class,
                MockRepository.Plugin.class
            ),
            Function.identity()
        );
        secondCluster.beforeTest(random());
    }

    @After
    public void stopSecondCluster() throws IOException {
        IOUtils.close(secondCluster);
    }

    public void testConcurrentDeleteFromOtherCluster() throws InterruptedException {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoNameOnFirstCluster = "test-repo";
        final String repoNameOnSecondCluster = randomBoolean() ? "test-repo" : "other-repo";
        createRepository(repoNameOnFirstCluster, "fs", repoPath);

        secondCluster.startClusterManagerOnlyNode();
        secondCluster.startDataOnlyNode();
        secondCluster.client()
            .admin()
            .cluster()
            .preparePutRepository(repoNameOnSecondCluster)
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath))
            .get();

        createIndexWithRandomDocs("test-idx-1", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-1");
        createIndexWithRandomDocs("test-idx-2", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-2");
        createIndexWithRandomDocs("test-idx-3", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-3");

        secondCluster.client().admin().cluster().prepareDeleteSnapshot(repoNameOnSecondCluster, "snap-1").get();
        secondCluster.client().admin().cluster().prepareDeleteSnapshot(repoNameOnSecondCluster, "snap-2").get();

        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoNameOnFirstCluster, "snap-4")
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(sne.getMessage(), containsString("failed to update snapshot in repository"));
        final RepositoryException cause = (RepositoryException) sne.getCause();
        assertThat(
            cause.getMessage(),
            containsString(
                "["
                    + repoNameOnFirstCluster
                    + "] concurrent modification of the index-N file, expected current generation [2] but it was not found in the repository"
            )
        );
        assertAcked(client().admin().cluster().prepareDeleteRepository(repoNameOnFirstCluster).get());
        createRepository(repoNameOnFirstCluster, "fs", repoPath);
        createFullSnapshot(repoNameOnFirstCluster, "snap-5");
    }
}

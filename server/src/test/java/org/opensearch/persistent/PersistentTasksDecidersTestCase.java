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

package org.opensearch.persistent;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public abstract class PersistentTasksDecidersTestCase extends OpenSearchTestCase {

    /** Needed by {@link ClusterService} **/
    private static ThreadPool threadPool;
    /** Needed by {@link PersistentTasksClusterService} **/
    private ClusterService clusterService;

    private PersistentTasksClusterService persistentTasksClusterService;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestClass().getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(emptyList()) {
            @Override
            public <Params extends PersistentTaskParams> PersistentTasksExecutor<Params> getPersistentTaskExecutorSafe(String taskName) {
                return new PersistentTasksExecutor<Params>(taskName, null) {
                    @Override
                    protected void nodeOperation(AllocatedPersistentTask task, Params params, PersistentTaskState state) {
                        logger.debug("Executing task {}", task);
                    }
                };
            }
        };
        persistentTasksClusterService = new PersistentTasksClusterService(
            clusterService.getSettings(),
            registry,
            clusterService,
            threadPool
        );
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    protected ClusterState reassign(final ClusterState clusterState) {
        return persistentTasksClusterService.reassignTasks(clusterState);
    }

    protected void updateSettings(final Settings settings) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        Settings.Builder updated = Settings.builder();
        clusterSettings.updateDynamicSettings(settings, updated, Settings.builder(), getTestClass().getName());
        clusterSettings.applySettings(updated.build());
    }

    protected static ClusterState createClusterStateWithTasks(final int nbNodes, final int nbTasks) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < nbNodes; i++) {
            nodes.add(new DiscoveryNode("_node_" + i, buildNewFakeTransportAddress(), Version.CURRENT));
        }

        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        for (int i = 0; i < nbTasks; i++) {
            tasks.addTask("_task_" + i, "test", null, new PersistentTasksCustomMetadata.Assignment(null, "initialized"));
        }

        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build()).build();

        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).build();
    }

    /** Asserts that the given cluster state contains nbTasks tasks that are assigned **/
    protected static void assertNbAssignedTasks(final long nbTasks, final ClusterState clusterState) {
        assertPersistentTasks(nbTasks, clusterState, PersistentTasksCustomMetadata.PersistentTask::isAssigned);
    }

    /** Asserts that the given cluster state contains nbTasks tasks that are NOT assigned **/
    protected static void assertNbUnassignedTasks(final long nbTasks, final ClusterState clusterState) {
        assertPersistentTasks(nbTasks, clusterState, task -> task.isAssigned() == false);
    }

    /** Asserts that the cluster state contains nbTasks tasks that verify the given predicate **/
    protected static void assertPersistentTasks(
        final long nbTasks,
        final ClusterState clusterState,
        final Predicate<PersistentTasksCustomMetadata.PersistentTask> predicate
    ) {
        PersistentTasksCustomMetadata tasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        assertNotNull("Persistent tasks must be not null", tasks);
        assertEquals(nbTasks, tasks.tasks().stream().filter(predicate).count());
    }
}

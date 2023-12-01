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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.service;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.LocalNodeMasterListener;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.node.Node;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;

/**
 * Main Cluster Service
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterService extends AbstractLifecycleComponent {
    private final ClusterManagerService clusterManagerService;

    private final ClusterApplierService clusterApplierService;

    public static final org.opensearch.common.settings.Setting.AffixSetting<String> USER_DEFINED_METADATA = Setting.prefixKeySetting(
        "cluster.metadata.",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    /**
     * The node's settings.
     */
    private final Settings settings;

    private final ClusterName clusterName;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    private final String nodeName;

    private RerouteService rerouteService;

    private IndexingPressureService indexingPressureService;

    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this(
            settings,
            clusterSettings,
            new ClusterManagerService(settings, clusterSettings, threadPool),
            new ClusterApplierService(Node.NODE_NAME_SETTING.get(settings), settings, clusterSettings, threadPool)
        );
    }

    public ClusterService(
        Settings settings,
        ClusterSettings clusterSettings,
        ClusterManagerService clusterManagerService,
        ClusterApplierService clusterApplierService
    ) {
        this.settings = settings;
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterManagerService = clusterManagerService;
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        // Add a no-op update consumer so changes are logged
        this.clusterSettings.addAffixUpdateConsumer(USER_DEFINED_METADATA, (first, second) -> {}, (first, second) -> {});
        this.clusterApplierService = clusterApplierService;
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        clusterApplierService.setNodeConnectionsService(nodeConnectionsService);
    }

    public void setRerouteService(RerouteService rerouteService) {
        assert this.rerouteService == null : "RerouteService is already set";
        this.rerouteService = rerouteService;
    }

    public RerouteService getRerouteService() {
        assert this.rerouteService != null : "RerouteService not set";
        return rerouteService;
    }

    @Override
    protected synchronized void doStart() {
        clusterApplierService.start();
        clusterManagerService.start();
    }

    @Override
    protected synchronized void doStop() {
        clusterManagerService.stop();
        clusterApplierService.stop();
    }

    @Override
    protected synchronized void doClose() {
        clusterManagerService.close();
        clusterApplierService.close();
    }

    /**
     * The local node.
     */
    public DiscoveryNode localNode() {
        DiscoveryNode localNode = state().getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    /**
     * The currently applied cluster state.
     * TODO: Should be renamed to appliedState / appliedClusterState
     */
    public ClusterState state() {
        return clusterApplierService.state();
    }

    /**
     * Adds a high priority applier of updated cluster states.
     */
    public void addHighPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addHighPriorityApplier(applier);
    }

    /**
     * Adds an applier which will be called after all high priority and normal appliers have been called.
     */
    public void addLowPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addLowPriorityApplier(applier);
    }

    /**
     * Adds a applier of updated cluster states.
     */
    public void addStateApplier(ClusterStateApplier applier) {
        clusterApplierService.addStateApplier(applier);
    }

    /**
     * Removes an applier of updated cluster states.
     */
    public void removeApplier(ClusterStateApplier applier) {
        clusterApplierService.removeApplier(applier);
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(ClusterStateListener listener) {
        clusterApplierService.addListener(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(ClusterStateListener listener) {
        clusterApplierService.removeListener(listener);
    }

    /**
     * Add a listener for on/off local node cluster-manager events
     */
    public void addLocalNodeClusterManagerListener(LocalNodeClusterManagerListener listener) {
        clusterApplierService.addLocalNodeClusterManagerListener(listener);
    }

    /**
     * Add a listener for on/off local node cluster-manager events
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #addLocalNodeClusterManagerListener}
     */
    @Deprecated
    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        addLocalNodeClusterManagerListener(listener);
    }

    public ClusterManagerService getClusterManagerService() {
        return clusterManagerService;
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #getClusterManagerService()} */
    @Deprecated
    public MasterService getMasterService() {
        return clusterManagerService;
    }

    /**
     * Getter and Setter for IndexingPressureService, This method exposes IndexingPressureService stats to other plugins for usage.
     * Although Indexing Pressure instances can be accessed via Node and NodeService class but none of them are
     * present in the createComponents signature of Plugin interface currently. {@link org.opensearch.plugins.Plugin#createComponents}
     * Going forward, IndexingPressureService will have required constructs for exposing listeners/interfaces for plugin development.(#478)
     */
    public void setIndexingPressureService(IndexingPressureService indexingPressureService) {
        this.indexingPressureService = indexingPressureService;
    }

    public IndexingPressureService getIndexingPressureService() {
        return indexingPressureService;
    }

    public ClusterApplierService getClusterApplierService() {
        return clusterApplierService;
    }

    public static boolean assertClusterOrClusterManagerStateThread() {
        assert Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME)
            || Thread.currentThread().getName().contains(ClusterManagerService.CLUSTER_MANAGER_UPDATE_THREAD_NAME)
            : "not called from the master/cluster state update thread";
        return true;
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #assertClusterOrClusterManagerStateThread} */
    @Deprecated
    public static boolean assertClusterOrMasterStateThread() {
        return assertClusterOrClusterManagerStateThread();
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    /**
     * The node's settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * The name of this node.
     */
    public final String getNodeName() {
        return nodeName;
    }

    /**
     * Functionality for register task key to cluster manager node.
     *
     * @param taskKey - task key of task
     * @param throttlingEnabled - throttling is enabled for task or not i.e does data node perform retries on it or not
     * @return throttling task key which needs to be passed while submitting task to cluster manager
     */
    public ClusterManagerTaskThrottler.ThrottlingKey registerClusterManagerTask(String taskKey, boolean throttlingEnabled) {
        return clusterManagerService.registerClusterManagerTask(taskKey, throttlingEnabled);
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener> void submitStateUpdateTask(
        String source,
        T updateTask
    ) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTask(
        String source,
        T task,
        ClusterStateTaskConfig config,
        ClusterStateTaskExecutor<T> executor,
        ClusterStateTaskListener listener
    ) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(
        final String source,
        final Map<T, ClusterStateTaskListener> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    ) {
        clusterManagerService.submitStateUpdateTasks(source, tasks, config, executor);
    }
}

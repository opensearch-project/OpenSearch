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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContextAccess;
import org.opensearch.core.action.ActionListener;
import org.opensearch.node.Node;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.opensearch.test.OpenSearchTestCase.randomInt;
import static org.apache.lucene.tests.util.LuceneTestCase.random;

public class FakeThreadPoolClusterManagerService extends ClusterManagerService {
    private static final Logger logger = LogManager.getLogger(FakeThreadPoolClusterManagerService.class);

    private final String name;
    private final List<Runnable> pendingTasks = new ArrayList<>();
    private final Consumer<Runnable> onTaskAvailableToRun;
    private boolean scheduledNextTask = false;
    private boolean taskInProgress = false;
    private boolean waitForPublish = false;

    public FakeThreadPoolClusterManagerService(
        String nodeName,
        String serviceName,
        ThreadPool threadPool,
        Consumer<Runnable> onTaskAvailableToRun
    ) {
        super(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            new ClusterManagerMetrics(NoopMetricsRegistry.INSTANCE)
        );
        this.name = serviceName;
        this.onTaskAvailableToRun = onTaskAvailableToRun;
    }

    @Override
    protected PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
        return new PrioritizedOpenSearchThreadPoolExecutor(
            name,
            1,
            1,
            1,
            TimeUnit.SECONDS,
            OpenSearchExecutors.daemonThreadFactory(name),
            null,
            null
        ) {

            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                execute(command);
            }

            @Override
            public void execute(Runnable command) {
                pendingTasks.add(command);
                scheduleNextTaskIfNecessary();
            }
        };
    }

    public int getFakeMasterServicePendingTaskCount() {
        return pendingTasks.size();
    }

    private void scheduleNextTaskIfNecessary() {
        if (taskInProgress == false && pendingTasks.isEmpty() == false && scheduledNextTask == false) {
            scheduledNextTask = true;
            onTaskAvailableToRun.accept(new Runnable() {
                @Override
                public String toString() {
                    return "cluster-manager service scheduling next task";
                }

                @Override
                public void run() {
                    assert taskInProgress == false;
                    assert waitForPublish == false;
                    assert scheduledNextTask;
                    final int taskIndex = randomInt(pendingTasks.size() - 1);
                    logger.debug("next cluster-manager service task: choosing task {} of {}", taskIndex, pendingTasks.size());
                    final Runnable task = pendingTasks.remove(taskIndex);
                    taskInProgress = true;
                    scheduledNextTask = false;
                    final ThreadContext threadContext = threadPool.getThreadContext();
                    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                        ThreadContextAccess.doPrivilegedVoid(threadContext::markAsSystemContext);
                        task.run();
                    }
                    if (waitForPublish == false) {
                        taskInProgress = false;
                    }
                    FakeThreadPoolClusterManagerService.this.scheduleNextTaskIfNecessary();
                }
            });
        }
    }

    @Override
    public ClusterState.Builder incrementVersion(ClusterState clusterState) {
        // generate cluster UUID deterministically for repeatable tests
        return ClusterState.builder(clusterState).incrementVersion().stateUUID(UUIDs.randomBase64UUID(random()));
    }

    @Override
    protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis) {
        assert waitForPublish == false;
        waitForPublish = true;
        final AckListener ackListener = taskOutputs.createAckListener(threadPool, clusterChangedEvent.state());
        final ActionListener<Void> publishListener = new ActionListener<Void>() {

            private boolean listenerCalled = false;

            @Override
            public void onResponse(Void aVoid) {
                assert listenerCalled == false;
                listenerCalled = true;
                assert waitForPublish;
                waitForPublish = false;
                try {
                    onPublicationSuccess(clusterChangedEvent, taskOutputs);
                } finally {
                    taskInProgress = false;
                    scheduleNextTaskIfNecessary();
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert listenerCalled == false;
                listenerCalled = true;
                assert waitForPublish;
                waitForPublish = false;
                try {
                    onPublicationFailed(clusterChangedEvent, taskOutputs, startTimeMillis, e);
                } finally {
                    taskInProgress = false;
                    scheduleNextTaskIfNecessary();
                }
            }
        };
        threadPool.generic().execute(threadPool.getThreadContext().preserveContext(new Runnable() {
            @Override
            public void run() {
                clusterStatePublisher.publish(clusterChangedEvent, publishListener, wrapAckListener(ackListener));
            }

            @Override
            public String toString() {
                return "publish change of cluster state from version ["
                    + clusterChangedEvent.previousState().version()
                    + "] in term ["
                    + clusterChangedEvent.previousState().term()
                    + "] to version ["
                    + clusterChangedEvent.state().version()
                    + "] in term ["
                    + clusterChangedEvent.state().term()
                    + "]";
            }
        }));
    }

    protected AckListener wrapAckListener(AckListener ackListener) {
        return ackListener;
    }
}

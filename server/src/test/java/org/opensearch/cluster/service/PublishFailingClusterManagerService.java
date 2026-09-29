/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Fails the publication of every update whose batching summary the predicate accepts. The predicate sees
 * {@link ClusterChangedEvent#source()}, the {@link TaskBatcher} summary of the batch rather than the submitted source, so it
 * must match by containment, and below trace level the summary samples only the first 1000 tasks. It lives in this package
 * because {@code onPublicationFailed} and {@code TaskOutputs} are package-private.
 */
public class PublishFailingClusterManagerService extends FakeThreadPoolClusterManagerService {

    /** Tested against the publish's batching summary, not against a submitted source string - see the class javadoc. */
    private final Predicate<String> shouldFailPublish;

    public PublishFailingClusterManagerService(
        String nodeName,
        String serviceName,
        ThreadPool threadPool,
        Consumer<Runnable> onTaskAvailableToRun,
        Predicate<String> shouldFailPublish
    ) {
        super(nodeName, serviceName, threadPool, onTaskAvailableToRun);
        this.shouldFailPublish = shouldFailPublish;
    }

    @Override
    protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis) {
        if (shouldFailPublish.test(clusterChangedEvent.source())) {
            // Deliberately not delegating to super. The failure is reported here instead of through the publish
            // listener super installs, so super's private waitForPublish flag has to stay unset: returning without
            // touching it leaves the parent's task runner to clear taskInProgress and schedule the next task exactly
            // as it does for an update that never reached publication.
            onPublicationFailed(clusterChangedEvent, taskOutputs, startTimeMillis, new FailedToCommitClusterStateException("injected"));
            return;
        }
        super.publish(clusterChangedEvent, taskOutputs, startTimeMillis);
    }
}

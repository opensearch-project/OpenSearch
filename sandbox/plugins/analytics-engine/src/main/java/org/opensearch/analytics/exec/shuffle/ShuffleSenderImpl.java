/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataAction;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataRequest;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataResponse;
import org.opensearch.analytics.spi.ShuffleSender;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Framework implementation of {@link ShuffleSender} that wires backend partitioned sinks to the
 * shuffle transport without exposing transport types to backends. Stamps every send with
 * {@code (queryId, targetStageId, side)} so the receiver routes payloads into the correct
 * buffer slice on the consumer's node.
 *
 * <p>Backpressure handling: each send goes through {@link ShuffleSenderRetry} so a
 * {@code backpressureRejected=true} response triggers exponential-backoff retry. Backends never
 * see backpressure; they get a single {@link ActionListener#onResponse} on success or
 * {@link ActionListener#onFailure} on retry exhaustion / non-backpressure error.
 *
 * <p>Routing: the sender does not route by remote node directly — it dispatches via the local
 * node's {@link Client}, which the action's transport layer fans out to {@code targetWorkerNodeId}
 * through the standard {@code RemoteClusterClient} path. This is the same shape OLAP's
 * {@code ShuffleSender} uses and avoids duplicating connection lookup.
 *
 * @opensearch.internal
 */
public final class ShuffleSenderImpl implements ShuffleSender {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleSenderImpl.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final String queryId;
    private final int targetStageId;
    private final String side;

    public ShuffleSenderImpl(
        Client client,
        ThreadPool threadPool,
        ClusterService clusterService,
        String queryId,
        int targetStageId,
        String side
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.side = side;
    }

    @Override
    public void send(String targetWorkerNodeId, int partitionIndex, byte[] data, boolean isLast, ActionListener<Void> listener) {
        DiscoveryNode targetNode = clusterService.state().nodes().get(targetWorkerNodeId);
        if (targetNode == null) {
            listener.onFailure(
                new IllegalStateException(
                    "ShuffleSenderImpl: target node id '"
                        + targetWorkerNodeId
                        + "' not found in cluster state for queryId="
                        + queryId
                        + ", stage="
                        + targetStageId
                        + ", side="
                        + side
                        + ", partition="
                        + partitionIndex
                )
            );
            return;
        }

        AnalyticsShuffleDataRequest request = new AnalyticsShuffleDataRequest(
            queryId,
            targetStageId,
            side,
            partitionIndex,
            data == null || data.length == 0 ? null : data,
            isLast,
            targetWorkerNodeId
        );

        ShuffleSenderRetry.sendWithRetry(
            request,
            (req, l) -> client.execute(AnalyticsShuffleDataAction.INSTANCE, req, l),
            (delayMs, runnable) -> threadPool.schedule(
                runnable,
                org.opensearch.common.unit.TimeValue.timeValueMillis(delayMs),
                ThreadPool.Names.GENERIC
            ),
            new ActionListener<>() {
                @Override
                public void onResponse(AnalyticsShuffleDataResponse response) {
                    if (response.isBackpressureRejected()) {
                        // Retry budget exhausted on backpressure — surface as a failure so the
                        // sink stamps firstError and the producer fragment fails the query.
                        listener.onFailure(
                            new RuntimeException(
                                "Shuffle send rejected after retry exhaustion (queryId="
                                    + queryId
                                    + ", stage="
                                    + targetStageId
                                    + ", side="
                                    + side
                                    + ", partition="
                                    + partitionIndex
                                    + ", target="
                                    + targetWorkerNodeId
                                    + ")"
                            )
                        );
                        return;
                    }
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.debug(
                        "Shuffle send failed (queryId="
                            + queryId
                            + ", stage="
                            + targetStageId
                            + ", side="
                            + side
                            + ", partition="
                            + partitionIndex
                            + ", target="
                            + targetWorkerNodeId
                            + ")",
                        e
                    );
                    listener.onFailure(e);
                }
            }
        );
    }
}

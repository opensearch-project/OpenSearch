/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles incoming shuffle payloads on a worker node. Writes each partition's bytes into the
 * corresponding {@link ShuffleBufferManager.ShuffleBuffer}; returns {@code backpressureReject=true}
 * when the per-partition byte cap is exceeded so the sender retries with exponential backoff.
 * Ported from OLAP's {@code TransportShuffleDataAction}.
 *
 * @opensearch.internal
 */
public class TransportAnalyticsShuffleDataAction extends HandledTransportAction<AnalyticsShuffleDataRequest, AnalyticsShuffleDataResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAnalyticsShuffleDataAction.class);

    private final ShuffleBufferManager shuffleBufferManager;

    @Inject
    public TransportAnalyticsShuffleDataAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ShuffleBufferManager shuffleBufferManager
    ) {
        super(AnalyticsShuffleDataAction.NAME, transportService, actionFilters, AnalyticsShuffleDataRequest::new);
        this.shuffleBufferManager = shuffleBufferManager;
    }

    @Override
    protected void doExecute(Task task, AnalyticsShuffleDataRequest request, ActionListener<AnalyticsShuffleDataResponse> listener) {
        try {
            // Key the buffer by (queryId, stageId, partitionIndex). Nodes may host multiple
            // partitions of the same stage in small-cluster configurations; merging them here
            // would let one partition's consumer observe another partition's rows and isLast
            // markers, silently corrupting the join.
            ShuffleBufferManager.ShuffleBuffer buffer = shuffleBufferManager.getOrCreateBuffer(
                request.getQueryId(),
                request.getTargetStageId(),
                request.getPartitionIndex()
            );
            if (request.getData() != null) {
                boolean accepted = buffer.tryAddData(request.getSide(), request.getData());
                if (!accepted) {
                    logger.debug(
                        "Shuffle buffer full: query={}, stage={}, partition={}, side={}, current={}, cap={}",
                        request.getQueryId(),
                        request.getTargetStageId(),
                        request.getPartitionIndex(),
                        request.getSide(),
                        buffer.getCurrentBytes(),
                        buffer.getMaxBytes()
                    );
                    listener.onResponse(AnalyticsShuffleDataResponse.backpressureReject());
                    return;
                }
            }
            if (request.isLast()) {
                buffer.senderDone(request.getSide());
                logger.debug(
                    "Shuffle sender done: query={}, stage={}, partition={}, side={}",
                    request.getQueryId(),
                    request.getTargetStageId(),
                    request.getPartitionIndex(),
                    request.getSide()
                );
            }
            listener.onResponse(new AnalyticsShuffleDataResponse());
        } catch (Exception e) {
            logger.error("Failed to process shuffle data", e);
            listener.onFailure(e);
        }
    }
}

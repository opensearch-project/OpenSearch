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
import org.opensearch.analytics.spi.ShuffleBufferExceededException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Handles incoming shuffle payloads on a worker node. Admits each chunk against the node-level +
 * per-query on-heap byte budget ({@link ShuffleBufferManager#tryAdmit}) and, when accepted, writes
 * its bytes into the corresponding {@link ShuffleBufferManager.ShuffleBuffer}. Two budget outcomes
 * map to the wire protocol:
 * <ul>
 *   <li>node total momentarily over budget while the query still fits its share → returns
 *       {@code backpressureReject=true} so the sender retries with exponential backoff (the
 *       contention clears as other queries finish and release their buffers);</li>
 *   <li>this query's OWN footprint exceeds the per-query budget (can never fit, even on an idle
 *       node) → {@link ShuffleBufferExceededException} → {@code listener.onFailure} as a fail-fast,
 *       NON-retryable error.</li>
 * </ul>
 *
 * <p>When {@link AnalyticsShuffleDataRequest#getTargetNodeId} is non-null and not the local node,
 * forwards the request to the named target via {@link TransportService#sendRequest}. This
 * enables the producer-side {@code ShuffleSenderImpl} to use the local {@code Client} (which
 * always invokes the action on the calling node) without losing per-partition routing.
 *
 * @opensearch.internal
 */
public class TransportAnalyticsShuffleDataAction extends HandledTransportAction<AnalyticsShuffleDataRequest, AnalyticsShuffleDataResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAnalyticsShuffleDataAction.class);

    private final ShuffleBufferManager shuffleBufferManager;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportAnalyticsShuffleDataAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ShuffleBufferManager shuffleBufferManager,
        ClusterService clusterService
    ) {
        super(AnalyticsShuffleDataAction.NAME, transportService, actionFilters, AnalyticsShuffleDataRequest::new);
        this.shuffleBufferManager = shuffleBufferManager;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, AnalyticsShuffleDataRequest request, ActionListener<AnalyticsShuffleDataResponse> listener) {
        String targetNodeId = request.getTargetNodeId();
        String localNodeId = clusterService.localNode().getId();

        if (targetNodeId != null && !targetNodeId.equals(localNodeId)) {
            // Forward to the target worker node. Drop the targetNodeId on the forwarded copy so
            // the receiving handler executes locally rather than re-forwarding.
            DiscoveryNode targetNode = clusterService.state().nodes().get(targetNodeId);
            if (targetNode == null) {
                listener.onFailure(new IllegalStateException("Shuffle target node id '" + targetNodeId + "' not found in cluster state"));
                return;
            }
            AnalyticsShuffleDataRequest forwarded = new AnalyticsShuffleDataRequest(
                request.getQueryId(),
                request.getTargetStageId(),
                request.getSide(),
                request.getPartitionIndex(),
                request.getData(),
                request.isLast(),
                /* targetNodeId */ null
            );
            transportService.sendRequest(
                targetNode,
                AnalyticsShuffleDataAction.NAME,
                forwarded,
                new TransportResponseHandler<AnalyticsShuffleDataResponse>() {
                    @Override
                    public AnalyticsShuffleDataResponse read(StreamInput in) throws IOException {
                        return new AnalyticsShuffleDataResponse(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(AnalyticsShuffleDataResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }
                }
            );
            return;
        }

        try {
            // Key the buffer by (queryId, stageId, partitionIndex). Nodes may host multiple
            // partitions of the same stage in small-cluster configurations; merging them here
            // would let one partition's consumer observe another partition's rows and isLast
            // markers, silently corrupting the join.
            if (request.getData() != null) {
                // Admit against the NODE + PER-QUERY shuffle budgets (ShuffleBufferManager.tryAdmit):
                // - throws ShuffleBufferExceededException if THIS query's footprint alone exceeds the
                // per-query budget → caught below → listener.onFailure → terminal, NON-retryable
                // failure (the query can never fit; waiting won't help — the q17 OOM case);
                // - returns REJECT_RETRY if the NODE is momentarily over budget but this query still
                // fits its share → backpressureReject response → ShuffleSenderRetry backs off and
                // retries (room frees when other queries finish and release their buffers);
                // - ACCEPTED otherwise (bytes reserved + chunk stored).
                ShuffleBufferManager.AdmitResult admit = shuffleBufferManager.tryAdmit(
                    request.getQueryId(),
                    request.getTargetStageId(),
                    request.getPartitionIndex(),
                    request.getSide(),
                    request.getData()
                );
                if (admit == ShuffleBufferManager.AdmitResult.REJECT_RETRY) {
                    logger.debug(
                        "Shuffle node over budget (retryable): query={}, stage={}, partition={}, side={}, nodeTotal={}, nodeBudget={}",
                        request.getQueryId(),
                        request.getTargetStageId(),
                        request.getPartitionIndex(),
                        request.getSide(),
                        shuffleBufferManager.getTotalBytes(),
                        shuffleBufferManager.getNodeBudgetBytes()
                    );
                    listener.onResponse(AnalyticsShuffleDataResponse.backpressureReject());
                    return;
                }
            }
            if (request.isLast()) {
                ShuffleBufferManager.ShuffleBuffer buffer = shuffleBufferManager.getOrCreateBuffer(
                    request.getQueryId(),
                    request.getTargetStageId(),
                    request.getPartitionIndex()
                );
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
        } catch (ShuffleBufferExceededException e) {
            // Expected, deliberate fail-fast: this query's footprint alone exceeds the per-query
            // budget so it can never fit (the q17 case). Non-retryable; surface to the caller without
            // an ERROR-level stack trace (it's not a defect — debug-log the actionable detail only).
            logger.debug(
                "Shuffle query over per-query budget (fail-fast): query={}, stage={}, partition={}, side={}: {}",
                request.getQueryId(),
                request.getTargetStageId(),
                request.getPartitionIndex(),
                request.getSide(),
                e.getMessage()
            );
            listener.onFailure(e);
        } catch (Exception e) {
            logger.error("Failed to process shuffle data", e);
            listener.onFailure(e);
        }
    }
}

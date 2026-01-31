/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.indexmetadatacoordinator;

import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * A base class for operations that need to be performed on the Index Metadata Coordinator (IMC) node.
 *
 * @opensearch.internal
 */
public abstract class TransportIndexMetadataCoordinatorAction<Request extends ClusterManagerNodeRequest<Request>, Response extends ActionResponse>
    extends HandledTransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String executor;

    protected TransportIndexMetadataCoordinatorAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(actionName, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response read(StreamInput in) throws IOException;

    protected abstract void indexMetadataCoordinatorOperation(Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }

        ClusterState state = clusterService.state();
        String imcNodeId = state.nodes().getIndexMetadataCoordinatorNodeId();

        if (Strings.isNullOrEmpty(imcNodeId)) {
            listener.onFailure(new IllegalStateException("No Index Metadata Coordinator node found"));
            return;
        }

        logger.info("Found IMC Node selected - ", imcNodeId);

        if (imcNodeId.equals(clusterService.localNode().getId())) {
            // Execute locally - this is the IMC node
            executeLocally(task, request, state, listener);
        } else {
            // Route to IMC node
            DiscoveryNode imcNode = state.nodes().get(imcNodeId);
            if (imcNode == null) {
                listener.onFailure(new IllegalStateException("IMC node not found: " + imcNodeId));
                return;
            }

            logger.info("Sending request to IMC node");
            transportService.sendRequest(
                imcNode,
                actionName,
                request,
                new ActionListenerResponseHandler<Response>(listener, this::read)
            );
        }
    }

    private void executeLocally(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        final ClusterBlockException blockException = checkBlock(request, state);
        if (blockException != null) {
            listener.onFailure(blockException);
            return;
        }

        threadPool.executor(executor).execute(
            ActionRunnable.wrap(listener, l -> indexMetadataCoordinatorOperation(request, state, l))
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action for deleting point in time searches - supports deleting list and all point in time searches
 */
public class TransportDeletePitAction extends HandledTransportAction<DeletePitRequest, DeletePitResponse>
    implements
        TransportIndicesResolvingAction<DeletePitRequest> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final PitService pitService;

    @Inject
    public TransportDeletePitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        PitService pitService
    ) {
        super(DeletePitAction.NAME, transportService, actionFilters, DeletePitRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.pitService = pitService;
    }

    /**
     * Invoke 'delete all pits' or 'delete list of pits' workflow based on request
     */
    @Override
    protected void doExecute(Task task, DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
        if (isAllPitsRequest(request)) {
            deleteAllPits(listener);
        } else {
            deletePits(listener, request);
        }
    }

    @Override
    public OptionallyResolvedIndices resolveIndices(DeletePitRequest request) {
        if (isAllPitsRequest(request)) {
            return ResolvedIndices.unknown();
        } else {
            return ResolvedIndices.of(this.pitService.getIndicesForPitsFlat(request.getPitIds()));
        }
    }

    private boolean isAllPitsRequest(DeletePitRequest request) {
        return request.getPitIds().size() == 1 && "_all".equals(request.getPitIds().get(0));
    }

    /**
     * Deletes one or more point in time search contexts.
     */
    private void deletePits(ActionListener<DeletePitResponse> listener, DeletePitRequest request) {
        Map<String, List<PitSearchContextIdForNode>> nodeToContextsMap = new HashMap<>();
        // remove duplicates from the request
        Set<String> uniquePitIds = new LinkedHashSet<>(request.getPitIds());
        for (String pitId : uniquePitIds) {
            SearchContextId contextId = SearchContextId.decode(namedWriteableRegistry, pitId);
            for (SearchContextIdForNode contextIdForNode : contextId.shards().values()) {
                PitSearchContextIdForNode pitSearchContext = new PitSearchContextIdForNode(pitId, contextIdForNode);
                List<PitSearchContextIdForNode> contexts = nodeToContextsMap.getOrDefault(contextIdForNode.getNode(), new ArrayList<>());
                contexts.add(pitSearchContext);
                nodeToContextsMap.put(contextIdForNode.getNode(), contexts);
            }
        }
        pitService.deletePitContexts(nodeToContextsMap, listener);
    }

    /**
     * Delete all active PIT reader contexts leveraging list all PITs
     * <p>
     * For Cross cluster PITs :
     * - mixed cluster PITs ( PIT comprising local and remote ) will be fully deleted. Since there will atleast be
     * one reader context with PIT ID present in local cluster, 'Get all PITs' will retrieve the PIT ID with which
     * we can completely delete the PIT contexts in both local and remote cluster.
     * - fully remote PITs will not be deleted as 'Get all PITs' operates on local cluster only and no PIT info can
     * be retrieved when it's fully remote.
     */
    private void deleteAllPits(ActionListener<DeletePitResponse> listener) {
        // Get all PITs and execute delete operation for the PITs.
        pitService.getAllPits(ActionListener.wrap(getAllPitNodesResponse -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(
                getAllPitNodesResponse.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList())
            );
            deletePits(listener, deletePitRequest);
        }, listener::onFailure));
    }
}

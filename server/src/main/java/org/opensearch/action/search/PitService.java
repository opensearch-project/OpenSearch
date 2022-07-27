/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Service class for PIT reusable functions
 */
public class PitService {

    private static final Logger logger = LogManager.getLogger(PitService.class);

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;

    @Inject
    public PitService(ClusterService clusterService, SearchTransportService searchTransportService) {
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
    }

    /**
     * Delete list of pit contexts. Returns the details of success of operation per PIT ID.
     */
    public void deletePitContexts(
        Map<String, List<PitSearchContextIdForNode>> nodeToContextsMap,
        ActionListener<DeletePitResponse> listener
    ) {
        final Set<String> clusters = nodeToContextsMap.values()
            .stream()
            .flatMap(Collection::stream)
            .filter(ctx -> Strings.isEmpty(ctx.getSearchContextIdForNode().getClusterAlias()) == false)
            .map(c -> c.getSearchContextIdForNode().getClusterAlias())
            .collect(Collectors.toSet());
        StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = (StepListener<
            BiFunction<String, String, DiscoveryNode>>) SearchUtils.getConnectionLookupListener(
                searchTransportService.getRemoteClusterService(),
                clusterService.state(),
                clusters
            );
        lookupListener.whenComplete(nodeLookup -> {
            final GroupedActionListener<DeletePitResponse> groupedListener = getDeletePitGroupedListener(
                listener,
                nodeToContextsMap.size()
            );

            for (Map.Entry<String, List<PitSearchContextIdForNode>> entry : nodeToContextsMap.entrySet()) {
                String clusterAlias = entry.getValue().get(0).getSearchContextIdForNode().getClusterAlias();
                final DiscoveryNode node = nodeLookup.apply(clusterAlias, entry.getValue().get(0).getSearchContextIdForNode().getNode());
                if (node == null) {
                    logger.error(
                        () -> new ParameterizedMessage("node [{}] not found", entry.getValue().get(0).getSearchContextIdForNode().getNode())
                    );
                    List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                    for (PitSearchContextIdForNode pitSearchContextIdForNode : entry.getValue()) {
                        deletePitInfos.add(new DeletePitInfo(false, pitSearchContextIdForNode.getPitId()));
                    }
                    groupedListener.onResponse(new DeletePitResponse(deletePitInfos));
                } else {
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(clusterAlias, node);
                        searchTransportService.sendFreePITContexts(connection, entry.getValue(), groupedListener);
                    } catch (Exception e) {
                        logger.error(() -> new ParameterizedMessage("Delete PITs failed on node [{}]", node.getName()), e);
                        List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                        for (PitSearchContextIdForNode pitSearchContextIdForNode : entry.getValue()) {
                            deletePitInfos.add(new DeletePitInfo(false, pitSearchContextIdForNode.getPitId()));
                        }
                        groupedListener.onResponse(new DeletePitResponse(deletePitInfos));
                    }
                }
            }
        }, listener::onFailure);
    }

    public GroupedActionListener<DeletePitResponse> getDeletePitGroupedListener(ActionListener<DeletePitResponse> listener, int size) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(final Collection<DeletePitResponse> responses) {
                Map<String, Boolean> pitIdToSucceededMap = new HashMap<>();
                for (DeletePitResponse response : responses) {
                    for (DeletePitInfo deletePitInfo : response.getDeletePitResults()) {
                        if (!pitIdToSucceededMap.containsKey(deletePitInfo.getPitId())) {
                            pitIdToSucceededMap.put(deletePitInfo.getPitId(), deletePitInfo.isSuccessful());
                        }
                        if (!deletePitInfo.isSuccessful()) {
                            logger.debug(() -> new ParameterizedMessage("Deleting PIT with ID {} failed ", deletePitInfo.getPitId()));
                            pitIdToSucceededMap.put(deletePitInfo.getPitId(), deletePitInfo.isSuccessful());
                        }
                    }
                }
                List<DeletePitInfo> deletePitResults = new ArrayList<>();
                for (Map.Entry<String, Boolean> entry : pitIdToSucceededMap.entrySet()) {
                    deletePitResults.add(new DeletePitInfo(entry.getValue(), entry.getKey()));
                }
                DeletePitResponse deletePitResponse = new DeletePitResponse(deletePitResults);
                listener.onResponse(deletePitResponse);
            }

            @Override
            public void onFailure(final Exception e) {
                logger.error("Delete PITs failed", e);
                listener.onFailure(e);
            }
        }, size);
    }
}

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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.transport.RemoteClusterService;
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
 * Helper class for common search functions
 */
public class SearchUtils {
    private static final Logger logger = LogManager.getLogger(SearchUtils.class);

    public SearchUtils() {}

    /**
     * Get connection lookup listener for list of clusters passed
     */
    public static StepListener<BiFunction<String, String, DiscoveryNode>> getConnectionLookupListener(
        RemoteClusterService remoteClusterService,
        ClusterState state,
        Set<String> clusters
    ) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();

        if (clusters.isEmpty()) {
            lookupListener.onResponse((cluster, nodeId) -> state.getNodes().get(nodeId));
        } else {
            remoteClusterService.collectNodes(clusters, lookupListener);
        }
        return lookupListener;
    }

    /**
     * Delete list of pit contexts. Returns the details of success of operation per PIT ID.
     */
    public static void deletePitContexts(
        Map<String, List<PitSearchContextIdForNode>> nodeToContextsMap,
        ActionListener<DeletePitResponse> listener,
        ClusterState state,
        SearchTransportService searchTransportService
    ) {
        final Set<String> clusters = nodeToContextsMap.values()
            .stream()
            .flatMap(Collection::stream)
            .filter(ctx -> Strings.isEmpty(ctx.getSearchContextIdForNode().getClusterAlias()) == false)
            .map(c -> c.getSearchContextIdForNode().getClusterAlias())
            .collect(Collectors.toSet());
        StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = getConnectionLookupListener(
            searchTransportService.getRemoteClusterService(),
            state,
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

    public static GroupedActionListener<DeletePitResponse> getDeletePitGroupedListener(
        ActionListener<DeletePitResponse> listener,
        int size
    ) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(final Collection<DeletePitResponse> responses) {
                Map<String, Boolean> pitIdToSucceededMap = new HashMap<>();
                for (DeletePitResponse response : responses) {
                    for (DeletePitInfo deletePitInfo : response.getDeletePitResults()) {
                        if (!pitIdToSucceededMap.containsKey(deletePitInfo.getPitId())) {
                            pitIdToSucceededMap.put(deletePitInfo.getPitId(), deletePitInfo.isSucceeded());
                        }
                        if (!deletePitInfo.isSucceeded()) {
                            logger.debug(() -> new ParameterizedMessage("Deleting PIT with ID {} failed ", deletePitInfo.getPitId()));
                            pitIdToSucceededMap.put(deletePitInfo.getPitId(), deletePitInfo.isSucceeded());
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

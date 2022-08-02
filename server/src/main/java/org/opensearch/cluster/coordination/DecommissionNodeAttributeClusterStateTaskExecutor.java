/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.persistent.PersistentTasksCustomMetadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Decommissions and shuts down nodes having a given attribute and updates the cluster state
 *
 * @opensearch.internal
 */
public class DecommissionNodeAttributeClusterStateTaskExecutor
    implements
        ClusterStateTaskExecutor<DecommissionNodeAttributeClusterStateTaskExecutor.Task>,
        ClusterStateTaskListener {

    private final AllocationService allocationService;
    private final Logger logger;

    /**
     * Task for the executor.
     *
     * @opensearch.internal
     */
    public static class Task {

        private final DecommissionAttribute decommissionAttribute;
        private final String reason;

        public Task(final DecommissionAttribute decommissionAttribute, final String reason) {
            this.decommissionAttribute = decommissionAttribute;
            this.reason = reason;
        }

        public DecommissionAttribute decommissionAttribute() {
            return decommissionAttribute;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return "Decommission Node Attribute Task{"
                + "decommissionAttribute="
                + decommissionAttribute
                + ", reason='"
                + reason
                + '\''
                + '}';
        }
    }

    public DecommissionNodeAttributeClusterStateTaskExecutor(final AllocationService allocationService, final Logger logger) {
        this.allocationService = allocationService;
        this.logger = logger;
    }

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        List<DiscoveryNode> nodesToBeRemoved = new ArrayList<DiscoveryNode>();
        for (final Task task : tasks) {
            final Predicate<DiscoveryNode> shouldRemoveNodePredicate = discoveryNode -> nodeHasDecommissionedAttribute(discoveryNode, task);
            Iterator<DiscoveryNode> nodesIter = currentState.nodes().getNodes().valuesIt();
            while (nodesIter.hasNext()) {
                final DiscoveryNode node = nodesIter.next();
                if (shouldRemoveNodePredicate.test(node) && currentState.nodes().nodeExists(node)) {
                    nodesToBeRemoved.add(node);
                }
            }
        }
        if (nodesToBeRemoved.size() <= 0) {
            // no nodes to remove, will keep the current cluster state
            return ClusterTasksResult.<DecommissionNodeAttributeClusterStateTaskExecutor.Task>builder()
                .successes(tasks)
                .build(currentState);
        }
        for (DiscoveryNode nodeToBeRemoved : nodesToBeRemoved) {
            remainingNodesBuilder.remove(nodeToBeRemoved);
        }

        final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);

        return getTaskClusterTasksResult(currentState, tasks, remainingNodesClusterState);
    }

    private boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, Task task) {
        String discoveryNodeAttributeValue = discoveryNode.getAttributes().get(task.decommissionAttribute().attributeName());
        return discoveryNodeAttributeValue != null && task.decommissionAttribute().attributeValues().contains(discoveryNodeAttributeValue);
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    protected ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

    protected ClusterTasksResult<DecommissionNodeAttributeClusterStateTaskExecutor.Task> getTaskClusterTasksResult(
        ClusterState currentState,
        List<DecommissionNodeAttributeClusterStateTaskExecutor.Task> tasks,
        ClusterState remainingNodesClusterState
    ) {
        ClusterState ptasksDisassociatedState = PersistentTasksCustomMetadata.disassociateDeadNodes(remainingNodesClusterState);
        final ClusterTasksResult.Builder<DecommissionNodeAttributeClusterStateTaskExecutor.Task> resultBuilder = ClusterTasksResult.<
            DecommissionNodeAttributeClusterStateTaskExecutor.Task>builder().successes(tasks);
        return resultBuilder.build(allocationService.disassociateDeadNodes(ptasksDisassociatedState, true, describeTasks(tasks)));
    }

    @Override
    public void onFailure(final String source, final Exception e) {
        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
    }

    @Override
    public void onNoLongerClusterManager(String source) {
        logger.debug("no longer cluster-manager while decommissioning node attribute [{}]", source);
    }
}

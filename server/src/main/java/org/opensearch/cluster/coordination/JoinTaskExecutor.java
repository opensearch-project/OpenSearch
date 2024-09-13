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

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.persistent.PersistentTasksCustomMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.opensearch.cluster.decommission.DecommissionHelper.nodeCommissioned;
import static org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_CLUSTER_PUBLICATION_REPO_NAME_ATTRIBUTES;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.STRICT;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;

/**
 * Main executor for Nodes joining the OpenSearch cluster
 *
 * @opensearch.internal
 */
public class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinTaskExecutor.Task> {

    private final AllocationService allocationService;

    private static Logger logger = LogManager.getLogger(JoinTaskExecutor.class);
    private final RerouteService rerouteService;

    private final RemoteStoreNodeService remoteStoreNodeService;

    /**
     * Task for the join task executor.
     *
     * @opensearch.internal
     */
    public static class Task {

        private final DiscoveryNode node;
        private final String reason;

        public Task(DiscoveryNode node, String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return node != null ? node + " " + reason : reason;
        }

        public boolean isBecomeClusterManagerTask() {
            return reason.equals(BECOME_MASTER_TASK_REASON) || reason.equals(BECOME_CLUSTER_MANAGER_TASK_REASON);
        }

        /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #isBecomeClusterManagerTask()} */
        @Deprecated
        public boolean isBecomeMasterTask() {
            return isBecomeClusterManagerTask();
        }

        public boolean isFinishElectionTask() {
            return reason.equals(FINISH_ELECTION_TASK_REASON);
        }

        /**
         * @deprecated As of 2.0, because supporting inclusive language, replaced by {@link #BECOME_CLUSTER_MANAGER_TASK_REASON}
         */
        @Deprecated
        private static final String BECOME_MASTER_TASK_REASON = "_BECOME_MASTER_TASK_";
        private static final String BECOME_CLUSTER_MANAGER_TASK_REASON = "_BECOME_CLUSTER_MANAGER_TASK_";
        private static final String FINISH_ELECTION_TASK_REASON = "_FINISH_ELECTION_";
    }

    public JoinTaskExecutor(
        Settings settings,
        AllocationService allocationService,
        Logger logger,
        RerouteService rerouteService,
        RemoteStoreNodeService remoteStoreNodeService
    ) {
        this.allocationService = allocationService;
        JoinTaskExecutor.logger = logger;
        this.rerouteService = rerouteService;
        this.remoteStoreNodeService = remoteStoreNodeService;
    }

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> joiningNodes) throws Exception {
        final ClusterTasksResult.Builder<Task> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        boolean nodesChanged = false;
        ClusterState.Builder newState;
        DiscoveryNode remotePublishNode = null;

        if (joiningNodes.size() == 1 && joiningNodes.get(0).isFinishElectionTask()) {
            return results.successes(joiningNodes).build(currentState);
        } else if (currentNodes.getClusterManagerNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeClusterManagerTask)) {
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask) : "becoming a cluster-manager but election is not finished "
                + joiningNodes;
            // use these joins to try and become the cluster-manager.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough
            newState = becomeClusterManagerAndTrimConflictingNodes(currentState, joiningNodes);
            if (currentNodes.getLocalNode().isRemoteStatePublicationEnabled()) {
                remotePublishNode = currentNodes.getLocalNode();
            }
            nodesChanged = true;
        } else if (currentNodes.isLocalNodeElectedClusterManager() == false) {
            logger.trace(
                "processing node joins, but we are not the cluster-manager. current cluster-manager: {}",
                currentNodes.getClusterManagerNode()
            );
            throw new NotClusterManagerException("Node [" + currentNodes.getLocalNode() + "] not cluster-manager for join request");
        } else {
            newState = ClusterState.builder(currentState);
        }

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

        // An optimization can be done as this will get invoked
        // for every set of node join task which we can optimize to not compute if cluster state already has
        // repository information.
        Optional<DiscoveryNode> remoteDN = currentNodes.getNodes().values().stream().filter(DiscoveryNode::isRemoteStoreNode).findFirst();
        RepositoriesMetadata repositoriesMetadata = null;

        if (remotePublishNode != null) {
            repositoriesMetadata = remoteStoreNodeService.updateRepositoriesMetadata(
                remotePublishNode,
                currentState.getMetadata().custom(RepositoriesMetadata.TYPE)
            );
        } else {
            DiscoveryNode dn = remoteDN.orElseGet(() -> (currentNodes.getNodes().values()).stream().findFirst().get());
            repositoriesMetadata = remoteStoreNodeService.updateRepositoriesMetadata(
                dn,
                currentState.getMetadata().custom(RepositoriesMetadata.TYPE)
            );
        }
        assert nodesBuilder.isLocalNodeElectedClusterManager();

        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // we only enforce major version transitions on a fully formed clusters
        final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // processing any joins
        Map<String, String> joiniedNodeNameIds = new HashMap<>();
        for (final Task joinTask : joiningNodes) {
            final DiscoveryNode node = joinTask.node();
            if (joinTask.isBecomeClusterManagerTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else if (currentNodes.nodeExistsWithSameRoles(node)) {
                logger.debug("received a join request for an existing node [{}]", node);
            } else {
                try {
                    if (enforceMajorVersion) {
                        ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                    }
                    ensureNodesCompatibility(node, currentNodes, currentState.metadata(), minClusterNodeVersion, maxClusterNodeVersion);
                    // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                    // we have to reject nodes that don't support all indices we have in this cluster
                    ensureIndexCompatibility(node.getVersion(), currentState.getMetadata());
                    // we have added the same check in handleJoinRequest method and adding it here as this method
                    // would guarantee that a decommissioned node would never be able to join the cluster and ensures correctness
                    ensureNodeCommissioned(node, currentState.metadata());
                    nodesBuilder.add(node);

                    if (remoteDN.isEmpty() && node.isRemoteStoreNode()) {
                        // This is hit only on cases where we encounter first remote node
                        logger.info("Updating system repository now for remote store");
                        repositoriesMetadata = remoteStoreNodeService.updateRepositoriesMetadata(
                            node,
                            currentState.getMetadata().custom(RepositoriesMetadata.TYPE)
                        );
                    }

                    nodesChanged = true;
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                    if (node.isClusterManagerNode()) {
                        joiniedNodeNameIds.put(node.getName(), node.getId());
                    }
                } catch (IllegalArgumentException | IllegalStateException | NodeDecommissionedException e) {
                    results.failure(joinTask, e);
                    continue;
                }
            }
            results.success(joinTask);
        }

        if (nodesChanged) {
            rerouteService.reroute(
                "post-join reroute",
                Priority.HIGH,
                ActionListener.wrap(r -> logger.trace("post-join reroute completed"), e -> logger.debug("post-join reroute failed", e))
            );

            if (joiniedNodeNameIds.isEmpty() == false) {
                Set<CoordinationMetadata.VotingConfigExclusion> currentVotingConfigExclusions = currentState.getVotingConfigExclusions();
                Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusions = currentVotingConfigExclusions.stream()
                    .map(e -> {
                        // Update nodeId in VotingConfigExclusion when a new node with excluded node name joins
                        if (CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER.equals(e.getNodeId())
                            && joiniedNodeNameIds.containsKey(e.getNodeName())) {
                            return new CoordinationMetadata.VotingConfigExclusion(joiniedNodeNameIds.get(e.getNodeName()), e.getNodeName());
                        } else {
                            return e;
                        }
                    })
                    .collect(Collectors.toSet());

                // if VotingConfigExclusions did get updated
                if (newVotingConfigExclusions.equals(currentVotingConfigExclusions) == false) {
                    CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder(currentState.coordinationMetadata())
                        .clearVotingConfigExclusions();
                    newVotingConfigExclusions.forEach(coordMetadataBuilder::addVotingConfigExclusion);
                    Metadata newMetadata = Metadata.builder(currentState.metadata())
                        .coordinationMetadata(coordMetadataBuilder.build())
                        .build();
                    return results.build(
                        allocationService.adaptAutoExpandReplicas(
                            newState.nodes(nodesBuilder)
                                .metadata(updateMetadataWithRepositoriesMetadata(newMetadata, repositoriesMetadata))
                                .build()
                        )
                    );
                }
            }

            return results.build(
                allocationService.adaptAutoExpandReplicas(
                    newState.nodes(nodesBuilder)
                        .metadata(updateMetadataWithRepositoriesMetadata(currentState.metadata(), repositoriesMetadata))
                        .build()
                )
            );
        } else {
            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a cluster-manager
            return results.build(
                newState.metadata(updateMetadataWithRepositoriesMetadata(currentState.metadata(), repositoriesMetadata)).build()
            );
        }
    }

    private Metadata updateMetadataWithRepositoriesMetadata(Metadata currentMetadata, RepositoriesMetadata repositoriesMetadata) {
        if (repositoriesMetadata == null || repositoriesMetadata.repositories() == null || repositoriesMetadata.repositories().isEmpty()) {
            return currentMetadata;
        } else {
            return Metadata.builder(currentMetadata).putCustom(RepositoriesMetadata.TYPE, repositoriesMetadata.get()).build();
        }
    }

    protected ClusterState.Builder becomeClusterManagerAndTrimConflictingNodes(ClusterState currentState, List<Task> joiningNodes) {
        assert currentState.nodes().getClusterManagerNodeId() == null : currentState;
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        nodesBuilder.clusterManagerNodeId(currentState.nodes().getLocalNodeId());

        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeClusterManagerTask() || joinTask.isFinishElectionTask()) {
                // no-op
            } else {
                final DiscoveryNode joiningNode = joinTask.node();
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug(
                        "removing existing node [{}], which conflicts with incoming join from [{}]",
                        nodeWithSameAddress,
                        joiningNode
                    );
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }
        }

        // now trim any left over dead nodes - either left there when the previous cluster-manager stepped down
        // or removed by us above
        ClusterState tmpState = ClusterState.builder(currentState)
            .nodes(nodesBuilder)
            .blocks(
                ClusterBlocks.builder()
                    .blocks(currentState.blocks())
                    .removeGlobalBlock(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID)
            )
            .build();
        logger.trace("becomeClusterManagerAndTrimConflictingNodes: {}", tmpState.nodes());
        allocationService.cleanCaches();
        tmpState = PersistentTasksCustomMetadata.disassociateDeadNodes(tmpState);
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    @Override
    public boolean runOnlyOnClusterManager() {
        // we validate that we are allowed to change the cluster state during cluster state processing
        return false;
    }

    /**
     * a task indicates that the current node should become master
     *
     * @deprecated As of 2.0, because supporting inclusive language, replaced by {@link #newBecomeClusterManagerTask()}
     */
    @Deprecated
    public static Task newBecomeMasterTask() {
        return new Task(null, Task.BECOME_MASTER_TASK_REASON);
    }

    /**
     * a task indicates that the current node should become cluster-manager
     */
    public static Task newBecomeClusterManagerTask() {
        return new Task(null, Task.BECOME_CLUSTER_MANAGER_TASK_REASON);
    }

    /**
     * a task that is used to signal the election is stopped and we should process pending joins.
     * it may be used in combination with {@link JoinTaskExecutor#newBecomeClusterManagerTask()}
     */
    public static Task newFinishElectionTask() {
        return new Task(null, Task.FINISH_ELECTION_TASK_REASON);
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of opensearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     *
     * @throws IllegalStateException if any index is incompatible with the given version
     * @see Version#minimumIndexCompatibilityVersion()
     */
    public static void ensureIndexCompatibility(final Version nodeVersion, Metadata metadata) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetadata idxMetadata : metadata) {
            if (idxMetadata.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException(
                    "index "
                        + idxMetadata.getIndex()
                        + " version not supported: "
                        + idxMetadata.getCreationVersion()
                        + " the node version is: "
                        + nodeVersion
                );
            }
            if (idxMetadata.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException(
                    "index "
                        + idxMetadata.getIndex()
                        + " version not supported: "
                        + idxMetadata.getCreationVersion()
                        + " minimum compatible index version is: "
                        + supportedIndexVersion
                );
            }
        }
    }

    /**
     * ensures that the joining node has a version that's compatible with all current nodes
     */
    public static void ensureNodesCompatibility(final DiscoveryNode joiningNode, DiscoveryNodes currentNodes, Metadata metadata) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNode, currentNodes, metadata, minNodeVersion, maxNodeVersion);
    }

    /**
     * ensures that the joining node has a version that's compatible with a given version range and ensures that the
     * joining node has required attributes to join a remotestore cluster.
     */
    public static void ensureNodesCompatibility(
        DiscoveryNode joiningNode,
        DiscoveryNodes currentNodes,
        Metadata metadata,
        Version minClusterNodeVersion,
        Version maxClusterNodeVersion
    ) {
        Version joiningNodeVersion = joiningNode.getVersion();
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException(
                "node version ["
                    + joiningNodeVersion
                    + "] is not supported. "
                    + "The cluster contains nodes with version ["
                    + maxClusterNodeVersion
                    + "], which is incompatible."
            );
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException(
                "node version ["
                    + joiningNodeVersion
                    + "] is not supported."
                    + "The cluster contains nodes with version ["
                    + minClusterNodeVersion
                    + "], which is incompatible."
            );
        }

        ensureRemoteRepositoryCompatibility(joiningNode, currentNodes, metadata);
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the cluster-manager/master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    public static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte clusterMajor = minClusterNodeVersion.major == 1 ? 7 : minClusterNodeVersion.major;
        if (joiningNodeVersion.compareMajor(minClusterNodeVersion) < 0) {
            throw new IllegalStateException(
                "node version ["
                    + joiningNodeVersion
                    + "] is not supported. "
                    + "All nodes in the cluster are of a higher major ["
                    + clusterMajor
                    + "]."
            );
        }
    }

    public static void ensureNodeCommissioned(DiscoveryNode node, Metadata metadata) {
        if (nodeCommissioned(node, metadata) == false) {
            throw new NodeDecommissionedException(
                "node [{}] has decommissioned attribute [{}] with current status of decommissioning [{}]",
                node.toString(),
                metadata.decommissionAttributeMetadata().decommissionAttribute().toString(),
                metadata.decommissionAttributeMetadata().status().status()
            );
        }
    }

    public static void ensureRemoteRepositoryCompatibility(DiscoveryNode joiningNode, DiscoveryNodes currentNodes, Metadata metadata) {
        List<DiscoveryNode> existingNodes = new ArrayList<>(currentNodes.getNodes().values());

        boolean isClusterRemoteStoreEnabled = existingNodes.stream().anyMatch(DiscoveryNode::isRemoteStoreNode);
        if (isClusterRemoteStoreEnabled || joiningNode.isRemoteStoreNode()) {
            ensureRemoteStoreNodesCompatibility(joiningNode, currentNodes, metadata);
        } else {
            ensureRemoteClusterStateNodesCompatibility(joiningNode, currentNodes);
        }
    }

    private static void ensureRemoteClusterStateNodesCompatibility(DiscoveryNode joiningNode, DiscoveryNodes currentNodes) {
        List<DiscoveryNode> existingNodes = new ArrayList<>(currentNodes.getNodes().values());

        assert existingNodes.isEmpty() == false;
        Optional<DiscoveryNode> remotePublicationNode = existingNodes.stream()
            .filter(DiscoveryNode::isRemoteStatePublicationEnabled)
            .findFirst();

        if (remotePublicationNode.isPresent() && joiningNode.isRemoteStatePublicationEnabled()) {
            ensureRepositoryCompatibility(joiningNode, remotePublicationNode.get(), REMOTE_CLUSTER_PUBLICATION_REPO_NAME_ATTRIBUTES);
        }
    }

    /**
     * The method ensures homogeneity -
     * 1. The joining node has to be a remote store backed if it's joining a remote store backed cluster. Validates
     * remote store attributes of joining node against the existing nodes of cluster.
     * 2. The joining node has to be a non-remote store backed if it is joining a non-remote store backed cluster.
     * Validates no remote store attributes are present in joining node as existing nodes in the cluster doesn't have
     * remote store attributes.
     * <p>
     * A remote store backed node is the one which holds all the remote store attributes and a remote store backed
     * cluster is the one which has only homogeneous remote store backed nodes with same node attributes
     * <p>
     * TODO: When we support moving from remote store cluster to non remote store and vice versa the this logic will
     *       needs to be modified.
     */
    private static void ensureRemoteStoreNodesCompatibility(DiscoveryNode joiningNode, DiscoveryNodes currentNodes, Metadata metadata) {

        List<DiscoveryNode> existingNodes = new ArrayList<>(currentNodes.getNodes().values());

        assert existingNodes.isEmpty() == false;

        CompatibilityMode remoteStoreCompatibilityMode = REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(metadata.settings());

        List<String> reposToSkip = new ArrayList<>(1);
        Optional<DiscoveryNode> remoteRoutingTableNode = existingNodes.stream()
            .filter(
                node -> node.getAttributes().get(RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY) != null
            )
            .findFirst();
        // If none of the existing nodes have routing table repo, then we skip this repo check if present in joining node.
        // This ensures a new node with remote routing table repo is able to join the cluster.
        if (remoteRoutingTableNode.isEmpty()) {
            String joiningNodeRepoName = joiningNode.getAttributes()
                .get(RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY);
            if (joiningNodeRepoName != null) {
                reposToSkip.add(joiningNodeRepoName);
            }
        }

        if (STRICT.equals(remoteStoreCompatibilityMode)) {
            DiscoveryNode existingNode = remoteRoutingTableNode.orElseGet(() -> existingNodes.get(0));
            if (joiningNode.isRemoteStoreNode()) {
                ensureRemoteStoreNodesCompatibility(joiningNode, existingNode, reposToSkip);
            } else {
                if (existingNode.isRemoteStoreNode()) {
                    throw new IllegalStateException(
                        "a non remote store node [" + joiningNode + "] is trying to join a remote store cluster"
                    );
                }
            }
        } else {
            if (MIXED.equals(remoteStoreCompatibilityMode)) {
                if (joiningNode.getVersion().after(currentNodes.getMaxNodeVersion())) {
                    String reason = String.format(
                        Locale.ROOT,
                        "remote migration : a node [%s] of higher version [%s] is not allowed to join a cluster with maximum version [%s]",
                        joiningNode,
                        joiningNode.getVersion(),
                        currentNodes.getMaxNodeVersion()
                    );
                    logger.warn(reason);
                    throw new IllegalStateException(reason);
                }
                if (joiningNode.isRemoteStoreNode()) {
                    Optional<DiscoveryNode> remoteDN = remoteRoutingTableNode.isPresent()
                        ? remoteRoutingTableNode
                        : existingNodes.stream().filter(DiscoveryNode::isRemoteStoreNode).findFirst();
                    remoteDN.ifPresent(discoveryNode -> ensureRemoteStoreNodesCompatibility(joiningNode, discoveryNode, reposToSkip));
                }
            }
        }
    }

    private static void ensureRemoteStoreNodesCompatibility(
        DiscoveryNode joiningNode,
        DiscoveryNode existingNode,
        List<String> reposToSkip
    ) {
        if (joiningNode.isRemoteStoreNode()) {
            if (existingNode.isRemoteStoreNode()) {
                RemoteStoreNodeAttribute joiningRemoteStoreNodeAttribute = new RemoteStoreNodeAttribute(joiningNode);
                RemoteStoreNodeAttribute existingRemoteStoreNodeAttribute = new RemoteStoreNodeAttribute(existingNode);
                if (existingRemoteStoreNodeAttribute.equalsWithRepoSkip(joiningRemoteStoreNodeAttribute, reposToSkip) == false) {
                    throw new IllegalStateException(
                        "a remote store node ["
                            + joiningNode
                            + "] is trying to join a remote store cluster with incompatible node attributes in "
                            + "comparison with existing node ["
                            + existingNode
                            + "]"
                    );
                }
            } else {
                throw new IllegalStateException("a remote store node [" + joiningNode + "] is trying to join a non remote store cluster");
            }
        }
    }

    private static void ensureRepositoryCompatibility(DiscoveryNode joiningNode, DiscoveryNode existingNode, List<String> reposToValidate) {

        RemoteStoreNodeAttribute joiningRemoteStoreNodeAttribute = new RemoteStoreNodeAttribute(joiningNode);
        RemoteStoreNodeAttribute existingRemoteStoreNodeAttribute = new RemoteStoreNodeAttribute(existingNode);

        if (existingRemoteStoreNodeAttribute.equalsForRepositories(joiningRemoteStoreNodeAttribute, reposToValidate) == false) {
            throw new IllegalStateException(
                "a remote store node ["
                    + joiningNode
                    + "] is trying to join a remote store cluster with incompatible node attributes in "
                    + "comparison with existing node ["
                    + existingNode
                    + "]"
            );
        }
    }

    public static Collection<BiConsumer<DiscoveryNode, ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators
    ) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node, state.getNodes(), state.metadata());
            ensureIndexCompatibility(node.getVersion(), state.getMetadata());
            ensureNodeCommissioned(node, state.getMetadata());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}

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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.opensearch.OpenSearchException;
import org.opensearch.cli.Terminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

/**
 * Tool to run an unsafe bootstrap
 *
 * @opensearch.internal
 */
public class UnsafeBootstrapClusterManagerCommand extends OpenSearchNodeCommand {

    static final String CLUSTER_STATE_TERM_VERSION_MSG_FORMAT = "Current node cluster state (term, version) pair is (%s, %s)";
    static final String CONFIRMATION_MSG = DELIMITER
        + "\n"
        + "You should only run this tool if you have permanently lost half or more\n"
        + "of the cluster-manager-eligible nodes in this cluster, and you cannot restore the\n"
        + "cluster from a snapshot. This tool can cause arbitrary data loss and its\n"
        + "use should be your last resort. If you have multiple surviving cluster-manager\n"
        + "eligible nodes, you should run this tool on the node with the highest\n"
        + "cluster state (term, version) pair.\n"
        + "\n"
        + "Do you want to proceed?\n";

    static final String NOT_CLUSTER_MANAGER_NODE_MSG = "unsafe-bootstrap tool can only be run on cluster-manager eligible node";

    static final String EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG =
        "last committed voting voting configuration is empty, cluster has never been bootstrapped?";

    static final String CLUSTER_MANAGER_NODE_BOOTSTRAPPED_MSG = "Cluster-manager node was successfully bootstrapped";
    static final Setting<String> UNSAFE_BOOTSTRAP = ClusterService.USER_DEFINED_METADATA.getConcreteSetting(
        "cluster.metadata.unsafe-bootstrap"
    );

    private OptionSpec<Boolean> applyClusterReadOnlyBlockOption;

    UnsafeBootstrapClusterManagerCommand() {
        super(
            "Forces the successful election of the current node after the permanent loss of the half or more cluster-manager-eligible nodes"
        );
        applyClusterReadOnlyBlockOption = parser.accepts("apply-cluster-read-only-block", "Optional cluster.blocks.read_only setting")
            .withOptionalArg()
            .ofType(Boolean.class);
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        terminal.println(Terminal.Verbosity.VERBOSE, "Checking node.roles setting");
        Boolean clusterManager = DiscoveryNode.isClusterManagerNode(settings);
        if (clusterManager == false) {
            throw new OpenSearchException(NOT_CLUSTER_MANAGER_NODE_MSG);
        }

        return true;
    }

    protected void processNodePaths(Terminal terminal, Path[] dataPaths, int nodeLockId, OptionSet options, Environment env)
        throws IOException {
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        final Tuple<Long, ClusterState> state = loadTermAndClusterState(persistedClusterStateService, env);
        final ClusterState oldClusterState = state.v2();

        final Metadata metadata = oldClusterState.metadata();

        final CoordinationMetadata coordinationMetadata = metadata.coordinationMetadata();
        if (coordinationMetadata == null
            || coordinationMetadata.getLastCommittedConfiguration() == null
            || coordinationMetadata.getLastCommittedConfiguration().isEmpty()) {
            throw new OpenSearchException(EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
        }
        terminal.println(
            String.format(Locale.ROOT, CLUSTER_STATE_TERM_VERSION_MSG_FORMAT, coordinationMetadata.term(), metadata.version())
        );

        CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
            .clearVotingConfigExclusions()
            .lastAcceptedConfiguration(
                new CoordinationMetadata.VotingConfiguration(Collections.singleton(persistedClusterStateService.getNodeId()))
            )
            .lastCommittedConfiguration(
                new CoordinationMetadata.VotingConfiguration(Collections.singleton(persistedClusterStateService.getNodeId()))
            )
            .build();

        Settings persistentSettings = Settings.builder().put(metadata.persistentSettings()).put(UNSAFE_BOOTSTRAP.getKey(), true).build();

        Boolean applyClusterReadOnlyBlock = applyClusterReadOnlyBlockOption.value(options);
        if (Objects.nonNull(applyClusterReadOnlyBlock)) {
            persistentSettings = Settings.builder()
                .put(persistentSettings)
                .put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), applyClusterReadOnlyBlock)
                .build();
        }

        Metadata.Builder newMetadata = Metadata.builder(metadata)
            .clusterUUID(Metadata.UNKNOWN_CLUSTER_UUID)
            .generateClusterUuidIfNeeded()
            .clusterUUIDCommitted(true)
            .persistentSettings(persistentSettings)
            .coordinationMetadata(newCoordinationMetadata);
        for (final IndexMetadata indexMetadata : metadata.indices().values()) {
            newMetadata.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(
                        Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())
                    )
            );
        }

        final ClusterState newClusterState = ClusterState.builder(oldClusterState).metadata(newMetadata).build();

        terminal.println(
            Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]"
        );

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(state.v1(), newClusterState);
        }

        terminal.println(CLUSTER_MANAGER_NODE_BOOTSTRAPPED_MSG);
    }
}

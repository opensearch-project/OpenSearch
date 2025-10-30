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
 * the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

import org.opensearch.cli.Terminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.env.Environment;
import org.opensearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Command to detach a node from the cluster
 *
 * @opensearch.internal
 */
public class DetachClusterCommand extends OpenSearchNodeCommand {

    static final String NODE_DETACHED_MSG = "Node was successfully detached from the cluster";

    static final String CONFIRMATION_MSG = DELIMITER + """

        You should only run this tool if you have permanently lost all of the
        cluster-manager-eligible nodes in this cluster and you cannot restore the cluster
        from a snapshot, or you have already unsafely bootstrapped a new cluster
        by running `opensearch-node unsafe-bootstrap` on a cluster-manager-eligible
        node that belonged to the same cluster as this node. This tool can cause
        arbitrary data loss and its use should be your last resort.

        Do you want to proceed?
        """;

    public DetachClusterCommand() {
        super("Detaches this node from its cluster, allowing it to unsafely join a new cluster");
    }

    @Override
    protected void processNodePaths(final Terminal terminal, final Path[] dataPaths, final int nodeLockId, final Environment env)
        throws IOException {
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final ClusterState oldClusterState = loadTermAndClusterState(persistedClusterStateService, env).v2();
        final ClusterState newClusterState = ClusterState.builder(oldClusterState)
            .metadata(updateMetadata(oldClusterState.metadata()))
            .build();
        terminal.println(
            Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]"
        );

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(updateCurrentTerm(), newClusterState);
        }

        terminal.println(NODE_DETACHED_MSG);
    }

    // package-private for tests
    static Metadata updateMetadata(final Metadata oldMetadata) {
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder()
            .lastAcceptedConfiguration(CoordinationMetadata.VotingConfiguration.MUST_JOIN_ELECTED_CLUSTER_MANAGER)
            .lastCommittedConfiguration(CoordinationMetadata.VotingConfiguration.MUST_JOIN_ELECTED_CLUSTER_MANAGER)
            .term(0)
            .build();
        return Metadata.builder(oldMetadata).coordinationMetadata(coordinationMetadata).clusterUUIDCommitted(false).build();
    }

    // package-private for tests
    static long updateCurrentTerm() {
        return 0L;
    }
}

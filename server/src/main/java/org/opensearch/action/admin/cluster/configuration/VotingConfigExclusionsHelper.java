/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.configuration;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.cluster.metadata.Metadata;

import java.util.Set;

import static org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING;

public class VotingConfigExclusionsHelper {

    public static ClusterState updateExclusionAndGetState(
        ClusterState currentState,
        Set<VotingConfigExclusion> resolvedExclusions,
        int finalMaxVotingConfigExclusions
    ) {
        final CoordinationMetadata.Builder builder = CoordinationMetadata.builder(currentState.coordinationMetadata());
        resolvedExclusions.forEach(builder::addVotingConfigExclusion);
        final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(builder.build()).build();
        final ClusterState newState = ClusterState.builder(currentState).metadata(newMetadata).build();
        assert newState.getVotingConfigExclusions().size() <= finalMaxVotingConfigExclusions;
        return newState;
    }

    // throws IAE if no nodes matched or maximum exceeded
    public static Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(
        AddVotingConfigExclusionsRequest request,
        ClusterState state,
        int maxVotingConfigExclusions
    ) {
        return request.resolveVotingConfigExclusionsAndCheckMaximum(
            state,
            maxVotingConfigExclusions,
            MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey()
        );
    }

    public static ClusterState clearExclusionsAndGetState(ClusterState currentState) {
        final CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(currentState.coordinationMetadata())
            .clearVotingConfigExclusions()
            .build();
        final Metadata newMetadata = Metadata.builder(currentState.metadata())
            .coordinationMetadata(newCoordinationMetadata)
            .build();
        return ClusterState.builder(currentState).metadata(newMetadata).build();
    }
}

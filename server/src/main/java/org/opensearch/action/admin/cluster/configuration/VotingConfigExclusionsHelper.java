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

/**
 * Static helper utilities for voting config exclusions cluster state updates
 *
 * @opensearch.internal
 */
public class VotingConfigExclusionsHelper {

    /**
     * Static helper to update current state with given resolved exclusions
     *
     * @param currentState current cluster state
     * @param resolvedExclusions resolved exclusions from the request
     * @param finalMaxVotingConfigExclusions max exclusions that be added
     * @return newly formed cluster state
     */
    public static ClusterState addExclusionAndGetState(
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

    /**
     * Resolves the exclusion from the request and throws IAE if no nodes matched or maximum exceeded
     *
     * @param request AddVotingConfigExclusionsRequest request
     * @param state current cluster state
     * @param maxVotingConfigExclusions max number of exclusion acceptable
     * @return set of VotingConfigExclusion
     */
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

    /**
     * Clears Voting config exclusion from the given cluster state
     *
     * @param currentState current cluster state
     * @return newly formed cluster state after clearing voting config exclusions
     */
    public static ClusterState clearExclusionsAndGetState(ClusterState currentState) {
        final CoordinationMetadata newCoordinationMetadata = CoordinationMetadata.builder(currentState.coordinationMetadata())
            .clearVotingConfigExclusions()
            .build();
        final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(newCoordinationMetadata).build();
        return ClusterState.builder(currentState).metadata(newMetadata).build();
    }
}

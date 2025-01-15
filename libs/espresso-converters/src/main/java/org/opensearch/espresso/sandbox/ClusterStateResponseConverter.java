/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.espresso.sandbox;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;

import com.oracle.truffle.espresso.polyglot.GuestTypeConversion;

/**
 * converter
 */
public class ClusterStateResponseConverter implements GuestTypeConversion<ClusterStateResponse> {
    /**
     * converter
     */
    @Override
    public ClusterStateResponse toGuest(Object polyglotInstance) {
        return new ClusterStateResponse(ClusterName.DEFAULT, ClusterState.EMPTY_STATE, false);
    }
}

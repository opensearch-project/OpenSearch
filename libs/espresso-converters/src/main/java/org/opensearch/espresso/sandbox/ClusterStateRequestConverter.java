/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.espresso.sandbox;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;

import com.oracle.truffle.espresso.polyglot.GuestTypeConversion;

/**
 * converter
 */
public class ClusterStateRequestConverter implements GuestTypeConversion<ClusterStateRequest> {
    /**
     * converter
     */
    @Override
    public ClusterStateRequest toGuest(Object polyglotInstance) {
        return new ClusterStateRequest();
    }
}

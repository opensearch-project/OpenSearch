/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.espresso.sandbox;

import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;

import com.oracle.truffle.espresso.polyglot.GuestTypeConversion;

/**
 * converter
 */
public class ClusterStateActionConverter implements GuestTypeConversion<ActionType<ClusterStateResponse>> {
    /**
     * converter
     */
    @Override
    public ActionType<ClusterStateResponse> toGuest(Object polyglotInstance) {
        return ClusterStateAction.INSTANCE;
    }
}

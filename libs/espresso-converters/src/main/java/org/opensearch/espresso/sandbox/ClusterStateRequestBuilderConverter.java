/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.espresso.sandbox;

import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.opensearch.client.OpenSearchClient;

import com.oracle.truffle.espresso.polyglot.ArityException;
import com.oracle.truffle.espresso.polyglot.GuestTypeConversion;
import com.oracle.truffle.espresso.polyglot.Interop;
import com.oracle.truffle.espresso.polyglot.UnknownIdentifierException;
import com.oracle.truffle.espresso.polyglot.UnsupportedMessageException;
import com.oracle.truffle.espresso.polyglot.UnsupportedTypeException;

/**
 * converter
 */
public class ClusterStateRequestBuilderConverter implements GuestTypeConversion<ClusterStateRequestBuilder> {
    /**
     * converter
     */
    @Override
    public ClusterStateRequestBuilder toGuest(Object polyglotInstance) {
        try {
            final OpenSearchClient client = Interop.invokeMemberWithCast(OpenSearchClient.class, polyglotInstance, "client");
            return new ClusterStateRequestBuilder(client, ClusterStateAction.INSTANCE);
        } catch (UnsupportedMessageException | ArityException | UnknownIdentifierException | UnsupportedTypeException e) {
            throw new RuntimeException(e);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A serializable implementation of principal for requests to extensions
 * Should be an authenticated user in the system
 */
public class Identity implements Principal {

    // For requests from unknown identity
    public static final AnonymousIdentity ANONYMOUS_IDENTITY = new AnonymousIdentity();

    private String principalIdentifier;
    private String username;

    private static final String NAME = "identity";

    public Identity(String id, String username) {
        this.principalIdentifier = id;
        this.username = username;
    }

    @Override
    public String getPrincipalIdentifier() {
        return principalIdentifier;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * TODO: Figure out what goes here if NamedWritable is in-fact required
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    /**
     * Anonymous identity is assumed when there is no user in the request
     */
    protected final static class AnonymousIdentity extends Identity {
        private final static String username = "Anonymous Panda";
        // TODO: generate identifier, should not be null
        private final static String ID = "";

        protected AnonymousIdentity() {
            super(ID, username);
        }
    }

}

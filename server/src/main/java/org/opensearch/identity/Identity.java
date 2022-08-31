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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A serializable implementation of principal for requests to extensions
 * Should be an authenticated user in the system
 */
public class Identity implements Principal {

    // For requests from unknown identity
    public static final AnonymousIdentity ANONYMOUS_IDENTITY = new AnonymousIdentity();

    private UUID id;
    private String username;
    private List<String> schemas;
    private Map<String, String> metadata;

    private static final String NAME = "identity";

    public Identity(UUID id, String username, List<String> schemas, Map<String, String> metadata) {
        this.id = id;
        this.username = username;
        this.schemas = schemas;
        this.metadata = metadata;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public List<String> getSchemas() {
        return schemas;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
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

    protected final static class AnonymousIdentity extends Identity {
        // TODO: Determine risk of collision when generating random UUID
        private final static UUID ID = new UUID(0x817a6e, 0x817a6e);
        private final static String username = "Anonymous Panda";
        private final static List<String> schemas = Collections.emptyList();
        private final static Map<String, String> metadata = Collections.emptyMap();

        protected AnonymousIdentity() {
            super(ID, username, schemas, metadata);
        }
    }

}

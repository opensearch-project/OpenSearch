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
    private String userName;
    private List<String> schemas;
    private Map<String, String> metaData;

    public Identity(UUID id, String userName, List<String> schemas, Map<String, String> metaData) {
        this.id = id;
        this.userName = userName;
        this.schemas = schemas;
        this.metaData = metaData;
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
    public String getUserName() {
        return userName;
    }

    @Override
    public Map<String, String> getMetaData() {
        return metaData;
    }

    protected final static class AnonymousIdentity extends Identity {
        // TODO: Determine risk of collision when generating random UUID
        private final static UUID ID = new UUID(0L, 0L);
        private final static String userName = "Anonymous Panda";
        private final static List<String> schemas = Collections.emptyList();
        private final static Map<String, String> metaData = Collections.emptyMap();

        protected AnonymousIdentity() {
            super(ID, userName, schemas, metaData);
        }
    }

}

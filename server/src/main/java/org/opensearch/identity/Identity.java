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

import org.opensearch.common.UUIDs;

import java.util.*;

public class Identity implements Principal{
    private UUID id;
    private String userName;
    private List<String> schemas;
    private Map<String, String> metaData;

    public Identity(UUID id, String userName, List<String> schemas, Map<String, String> metaData){
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

    protected final static class Anonymous extends Identity{
        private final static UUID ID = new UUID(0L, 0L);
        private final static String userName = "Anonymous Panda";
        private final static List<String> schemas = Collections.emptyList();
        private final static Map<String, String> metaData = Collections.emptyMap();

        protected Anonymous() {
            super(ID, userName, schemas, metaData);
        }
    }

}

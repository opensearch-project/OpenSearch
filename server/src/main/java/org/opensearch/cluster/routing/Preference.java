/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

/**
 * Routing Preference Type
 *
 * @opensearch.internal
 */
public enum Preference {

    /**
     * Route to specific shards
     */
    SHARDS("_shards"),

    /**
     * Route to preferred nodes, if possible
     */
    PREFER_NODES("_prefer_nodes"),

    /**
     * Route to local node, if possible
     */
    LOCAL("_local"),

    /**
     * Route to primary shards
     */
    PRIMARY("_primary"),

    /**
     * Route to replica shards
     */
    REPLICA("_replica"),

    /**
     * Route to primary shards first
     */
    PRIMARY_FIRST("_primary_first"),

    /**
     * Route to replica shards first
     */
    REPLICA_FIRST("_replica_first"),

    /**
     * Route to search replica shards
     */
    SEARCH_REPLICA("_search_replica"),

    /**
     * Route to the local shard only
     */
    ONLY_LOCAL("_only_local"),

    /**
     * Route to only node with attribute
     */
    ONLY_NODES("_only_nodes");

    private final String type;

    Preference(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }

    /**
     * Parses the Preference Type given a string
     */
    public static Preference parse(String preference) {
        String preferenceType;
        int colonIndex = preference.indexOf(':');
        if (colonIndex == -1) {
            preferenceType = preference;
        } else {
            preferenceType = preference.substring(0, colonIndex);
        }

        switch (preferenceType) {
            case "_shards":
                return SHARDS;
            case "_prefer_nodes":
                return PREFER_NODES;
            case "_local":
                return LOCAL;
            case "_primary":
                return PRIMARY;
            case "_replica":
                return REPLICA;
            case "_primary_first":
            case "_primaryFirst":
                return PRIMARY_FIRST;
            case "_replica_first":
            case "_replicaFirst":
                return REPLICA_FIRST;
            case "_only_local":
            case "_onlyLocal":
                return ONLY_LOCAL;
            case "_only_nodes":
                return ONLY_NODES;
            case "_search_replica":
                return SEARCH_REPLICA;
            default:
                throw new IllegalArgumentException("no Preference for [" + preferenceType + "]");
        }
    }

}

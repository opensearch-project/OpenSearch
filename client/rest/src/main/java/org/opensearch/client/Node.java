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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.client;

import org.apache.hc.core5.http.HttpHost;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata about an {@link HttpHost} running OpenSearch.
 */
public class Node {
    /**
     * Address that this host claims is its primary contact point.
     */
    private final HttpHost host;
    /**
     * Addresses on which the host is listening. These are useful to have
     * around because they allow you to find a host based on any address it
     * is listening on.
     */
    private final Set<HttpHost> boundHosts;
    /**
     * Name of the node as configured by the {@code node.name} attribute.
     */
    private final String name;
    /**
     * Version of OpenSearch that the node is running or {@code null}
     * if we don't know the version.
     */
    private final String version;
    /**
     * Roles that the OpenSearch process on the host has or {@code null}
     * if we don't know what roles the node has.
     */
    private final Roles roles;
    /**
     * Attributes declared on the node.
     */
    private final Map<String, List<String>> attributes;

    /**
     * Create a {@linkplain Node} with metadata. All parameters except
     * {@code host} are nullable and implementations of {@link NodeSelector}
     * need to decide what to do in their absence.
     *
     * @param host       primary host address
     * @param boundHosts addresses on which the host is listening
     * @param name       name of the node
     * @param version    version of OpenSearch
     * @param roles      roles that the OpenSearch process has on the host
     * @param attributes attributes declared on the node
     */
    public Node(HttpHost host, Set<HttpHost> boundHosts, String name, String version, Roles roles, Map<String, List<String>> attributes) {
        if (host == null) {
            throw new IllegalArgumentException("host cannot be null");
        }
        this.host = host;
        this.boundHosts = boundHosts;
        this.name = name;
        this.version = version;
        this.roles = roles;
        this.attributes = attributes;
    }

    /**
     * Create a {@linkplain Node} without any metadata.
     *
     * @param host primary host address
     */
    public Node(HttpHost host) {
        this(host, null, null, null, null, null);
    }

    /**
     * Contact information for the host.
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Addresses on which the host is listening. These are useful to have
     * around because they allow you to find a host based on any address it
     * is listening on.
     */
    public Set<HttpHost> getBoundHosts() {
        return boundHosts;
    }

    /**
     * The {@code node.name} of the node.
     */
    public String getName() {
        return name;
    }

    /**
     * Version of OpenSearch that the node is running or {@code null}
     * if we don't know the version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Roles that the OpenSearch process on the host has or {@code null}
     * if we don't know what roles the node has.
     */
    public Roles getRoles() {
        return roles;
    }

    /**
     * Attributes declared on the node.
     */
    public Map<String, List<String>> getAttributes() {
        return attributes;
    }

    /**
     * Convert node to string representation
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[host=").append(host);
        if (boundHosts != null) {
            b.append(", bound=").append(boundHosts);
        }
        if (name != null) {
            b.append(", name=").append(name);
        }
        if (version != null) {
            b.append(", version=").append(version);
        }
        if (roles != null) {
            b.append(", roles=").append(roles);
        }
        if (attributes != null) {
            b.append(", attributes=").append(attributes);
        }
        return b.append(']').toString();
    }

    /**
     * Compare two nodes for equality
     * @param obj node instance to compare with
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Node other = (Node) obj;
        return host.equals(other.host)
            && Objects.equals(boundHosts, other.boundHosts)
            && Objects.equals(name, other.name)
            && Objects.equals(version, other.version)
            && Objects.equals(roles, other.roles)
            && Objects.equals(attributes, other.attributes);
    }

    /**
     * Calculate the hash code of the node
     */
    @Override
    public int hashCode() {
        return Objects.hash(host, boundHosts, name, version, roles, attributes);
    }

    /**
     * Role information about an OpenSearch process.
     */
    public static final class Roles {

        private final Set<String> roles;

        /**
         * Create a {@link Roles} instance of the given string set.
         *
         * @param roles set of role names.
         */
        public Roles(final Set<String> roles) {
            this.roles = new TreeSet<>(roles);
        }

        /**
         * Returns whether or not the node <strong>could</strong> be elected cluster-manager.
         */
        public boolean isClusterManagerEligible() {
            return roles.contains("master") || roles.contains("cluster_manager");
        }

        /**
         * Returns whether or not the node stores data.
         */
        public boolean isData() {
            return roles.contains("data");
        }

        /**
         * Returns whether or not the node runs ingest pipelines.
         */
        public boolean isIngest() {
            return roles.contains("ingest");
        }

        /**
         * Returns whether the node is dedicated to provide search capability.
         */
        public boolean isSearch() {
            return roles.contains("search");
        }

        /**
         * Convert roles to string representation
         */
        @Override
        public String toString() {
            return String.join(",", roles);
        }

        /**
         * Compare two roles for equality
         * @param obj roles instance to compare with
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Roles other = (Roles) obj;
            return roles.equals(other.roles);
        }

        /**
         * Calculate the hash code of the roles
         */
        @Override
        public int hashCode() {
            return roles.hashCode();
        }

    }
}

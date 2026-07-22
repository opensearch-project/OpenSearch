/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Enhanced version for better readability, safety, and maintainability.
 */

package org.opensearch.client;

import org.apache.hc.core5.http.HttpHost;

import java.util.*;

/**
 * Represents metadata about a remote {@link HttpHost} running OpenSearch.
 * This class is immutable and thread-safe.
 */
public class Node {

    /** Primary host address for contacting the node. */
    private final HttpHost host;

    /** All network addresses the node is bound to. */
    private final Set<HttpHost> boundHosts;

    /** Logical OpenSearch node name. */
    private final String name;

    /** OpenSearch version running on the node, if known. */
    private final String version;

    /** Node role information, if available. */
    private final Roles roles;

    /** Custom node attributes. */
    private final Map<String, List<String>> attributes;

    /**
     * Creates a fully initialized Node instance.
     */
    public Node(HttpHost host,
                Set<HttpHost> boundHosts,
                String name,
                String version,
                Roles roles,
                Map<String, List<String>> attributes) {

        if (host == null) {
            throw new IllegalArgumentException("Host cannot be null");
        }

        this.host = host;
        this.boundHosts = boundHosts == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new HashSet<>(boundHosts));
        this.name = name;
        this.version = version;
        this.roles = roles;

        if (attributes != null) {
            Map<String, List<String>> copied = new HashMap<>();
            attributes.forEach((k, v) -> 
                copied.put(k, v == null ? null : Collections.unmodifiableList(new ArrayList<>(v)))
            );
            this.attributes = Collections.unmodifiableMap(copied);
        } else {
            this.attributes = Collections.emptyMap();
        }
    }

    /**
     * Creates a Node containing only the host address.
     */
    public Node(HttpHost host) {
        this(host, null, null, null, null, null);
    }

    // ------------------------------------
    // Getters
    // ------------------------------------

    public HttpHost getHost() {
        return host;
    }

    public Set<HttpHost> getBoundHosts() {
        return boundHosts;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public Roles getRoles() {
        return roles;
    }

    public Map<String, List<String>> getAttributes() {
        return attributes;
    }

    // ------------------------------------
    // String / Equality / Hashing
    // ------------------------------------

    @Override
    public String toString() {
        return "Node{" +
                "host=" + host +
                ", boundHosts=" + boundHosts +
                ", name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", roles=" + roles +
                ", attributes=" + attributes +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Node)) return false;
        Node other = (Node) obj;
        return Objects.equals(host, other.host)
                && Objects.equals(boundHosts, other.boundHosts)
                && Objects.equals(name, other.name)
                && Objects.equals(version, other.version)
                && Objects.equals(roles, other.roles)
                && Objects.equals(attributes, other.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, boundHosts, name, version, roles, attributes);
    }

    // ------------------------------------
    // Roles class
    // ------------------------------------

    /**
     * Represents functional roles of an OpenSearch node.
     */
    public static final class Roles {

        private final Set<String> roles;

        /**
         * Creates a new Roles object.
         */
        public Roles(final Set<String> roles) {
            this.roles = Collections.unmodifiableSet(new TreeSet<>(roles));
        }

        public boolean isClusterManagerEligible() {
            return roles.contains("master") || roles.contains("cluster_manager");
        }

        public boolean isData() {
            return roles.contains("data");
        }

        public boolean isIngest() {
            return roles.contains("ingest");
        }

        public boolean isSearch() {
            return roles.contains("search");
        }

        @Override
        public String toString() {
            return String.join(",", roles);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof Roles)) return false;
            Roles other = (Roles) obj;
            return roles.equals(other.roles);
        }

        @Override
        public int hashCode() {
            return roles.hashCode();
        }
    }

    // ------------------------------------
    // Builder for easier construction
    // ------------------------------------

    /**
     * Builder for constructing Node objects with optional metadata.
     */
    public static class Builder {
        private HttpHost host;
        private Set<HttpHost> boundHosts;
        private String name;
        private String version;
        private Roles roles;
        private Map<String, List<String>> attributes;

        public Builder host(HttpHost host) {
            this.host = host;
            return this;
        }

        public Builder boundHosts(Set<HttpHost> boundHosts) {
            this.boundHosts = boundHosts;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder roles(Roles roles) {
            this.roles = roles;
            return this;
        }

        public Builder attributes(Map<String, List<String>> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Node build() {
            return new Node(host, boundHosts, name, version, roles, attributes);
        }
    }
}

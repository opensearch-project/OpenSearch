/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.Objects;
import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Create a principal for a Node
 *
 * @opensearch.experimental
 */
public class NodePrincipal implements Principal {

    private final String name;

    /**
     * Creates a principal for a specific node
     * @param node A provided node
     */
    public NodePrincipal(final DiscoveryNode node) {
        this.name = node.getId();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        final Principal that = (Principal) obj;
        return Objects.equals(name, that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "NodePrincipal(" + "name=" + name + ")";
    }
}

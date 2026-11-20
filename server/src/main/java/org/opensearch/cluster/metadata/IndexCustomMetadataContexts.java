/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.metadata.Metadata.XContentContext;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Context declarations for built-in index custom metadata. Unregistered types retain their existing visibility in all contexts.
 * Declarations are independent of plugin installation so removing a plugin cannot remove a built-in privacy restriction.
 */
final class IndexCustomMetadataContexts {
    private static final Map<String, Set<XContentContext>> CONTEXTS = Map.of(
        IndexFieldDomainMetadata.CUSTOM_KEY,
        Set.of(XContentContext.GATEWAY, XContentContext.SNAPSHOT)
    );

    private IndexCustomMetadataContexts() {}

    static boolean isVisible(String type, XContentContext context) {
        Objects.requireNonNull(context, "context must not be null");
        Set<XContentContext> contexts = CONTEXTS.get(type);
        return contexts == null || contexts.contains(context);
    }
}

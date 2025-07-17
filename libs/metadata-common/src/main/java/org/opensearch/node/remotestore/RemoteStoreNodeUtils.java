/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Remote Store Node Utilities.
 *
 * @opensearch.internal
 */
public class RemoteStoreNodeUtils {

    private RemoteStoreNodeUtils() {}

    public static final List<String> REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = List.of("remote_store", "remote_publication");

    public static final List<String> REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".state.repository")
        .collect(Collectors.toList());

    public static final List<String> REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".routing_table.repository")
        .collect(Collectors.toList());

    public static final List<String> REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".segment.repository")
        .collect(Collectors.toList());

    public static final List<String> REMOTE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEYS = REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX.stream()
        .map(prefix -> prefix + ".translog.repository")
        .collect(Collectors.toList());

    public static boolean isClusterStateRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static boolean isRoutingTableRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static boolean isSegmentRepoConfigured(Map<String, String> attributes) {
        return containsKey(attributes, REMOTE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEYS);
    }

    public static boolean containsKey(Map<String, String> attributes, List<String> keys) {
        return keys.stream().filter(k -> attributes.containsKey(k)).findFirst().isPresent();
    }
}

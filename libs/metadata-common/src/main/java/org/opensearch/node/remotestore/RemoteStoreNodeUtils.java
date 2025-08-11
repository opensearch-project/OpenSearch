/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.opensearch.common.settings.Setting;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for remote backed storage nodes.
 *
 * @opensearch.internal
 */
public class RemoteStoreNodeUtils {

    private RemoteStoreNodeUtils() {}

    /**
     * If <code>true</code> connecting to remote clusters is supported on this node. If <code>false</code> this node will not establish
     * connections to any remote clusters configured. Search requests executed against this node (where this node is the coordinating node)
     * will fail if remote cluster syntax is used as an index pattern. The default is <code>true</code>
     */
    public static final Setting<Boolean> ENABLE_REMOTE_CLUSTERS = Setting.boolSetting(
        "cluster.remote.connect",
        true,
        Setting.Property.Deprecated,
        Setting.Property.NodeScope
    );

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

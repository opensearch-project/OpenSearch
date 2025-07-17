/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.common.settings.Setting;

import javax.net.ssl.SNIHostName;

/**
 * Settings related to a node.
 *
 * @opensearch.internal
 */
public class NodeSettings {

    // TODO: Move other node settings later to this class.

    /**
     * controls whether the node is allowed to persist things like metadata to disk
     * Note that this does not control whether the node stores actual indices (see
     * {NODE_DATA_SETTING}). However, if this is false, {NODE_DATA_SETTING}
     * and {NODE_MASTER_SETTING} must also be false.
     */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting(
        "node.local_storage",
        true,
        Setting.Property.Deprecated,
        Setting.Property.NodeScope
    );

    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Setting.Property.NodeScope);

    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting(
        "node.attr.",
        (key) -> new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " + "[" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e);
                }
            }
            return value;
        }, Setting.Property.NodeScope)
    );
}

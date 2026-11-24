/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.common.settings.SettingMetadata;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** Definition metadata for registered settings; contains neither configured values nor validation results.
 * <p>
 * Local-only: the response is built and rendered on the node that received the request and is deliberately not
 * serializable, so it can never be exchanged between nodes of different versions.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsResponse extends ActionResponse implements ToXContentObject {
    private final Map<String, SettingMetadata> settings;

    /** The complete result; entries are rendered in ascending key order regardless of the order they were supplied in. */
    public ClusterDescribeSettingsResponse(Map<String, SettingMetadata> settings) {
        this.settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    }

    /**
     * Not supported: the response is local-only. This constructor exists only because {@link org.opensearch.action.ActionType}
     * requires a response reader; it fails loudly if the response is ever routed over the transport layer.
     */
    public ClusterDescribeSettingsResponse(StreamInput in) {
        throw new UnsupportedOperationException("ClusterDescribeSettingsResponse is local-only and is not serializable");
    }

    public Map<String, SettingMetadata> settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("ClusterDescribeSettingsResponse is local-only and is not serializable");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startObject("settings");
        for (var entry : settings.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        return builder.endObject().endObject();
    }
}

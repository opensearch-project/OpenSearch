/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

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
 * @opensearch.internal
 */
public class ClusterDescribeSettingsResponse extends ActionResponse implements ToXContentObject {
    private final Map<String, SettingMetadata> settings;

    public ClusterDescribeSettingsResponse(Map<String, SettingMetadata> settings) {
        this.settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    }

    public ClusterDescribeSettingsResponse(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, SettingMetadata::new));
    }

    public Map<String, SettingMetadata> settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(settings, StreamOutput::writeString, (o, metadata) -> metadata.writeTo(o));
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

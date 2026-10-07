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

/** Dynamic-setting metadata; contains neither values nor defaults.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsResponse extends ActionResponse implements ToXContentObject {
    private final Map<String, Boolean> settings;

    public ClusterDescribeSettingsResponse(Map<String, Boolean> settings) {
        this.settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    }

    public ClusterDescribeSettingsResponse(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, StreamInput::readBoolean));
    }

    public Map<String, Boolean> settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(settings, StreamOutput::writeString, StreamOutput::writeBoolean);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startObject("settings");
        for (var entry : settings.entrySet()) {
            builder.startObject(entry.getKey()).field("dynamic", entry.getValue()).endObject();
        }
        return builder.endObject().endObject();
    }
}

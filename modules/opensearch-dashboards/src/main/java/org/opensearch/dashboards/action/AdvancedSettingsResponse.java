/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class AdvancedSettingsResponse extends ActionResponse implements ToXContentObject {

    private Map<String, Object> settings;

    public AdvancedSettingsResponse() {}

    public AdvancedSettingsResponse(Map<String, Object> settings) {
        this.settings = settings;
    }

    public AdvancedSettingsResponse(StreamInput in) throws IOException {
        super(in);
        this.settings = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(settings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (settings != null) {
            for (Map.Entry<String, Object> entry : settings.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }
}

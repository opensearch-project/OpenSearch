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

/**
 * Response for saved object CRUD operations. Returns the document source as a flat map.
 */
public class SavedObjectResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Object> source;

    public SavedObjectResponse(Map<String, Object> source) {
        this.source = source;
    }

    public SavedObjectResponse(StreamInput in) throws IOException {
        super(in);
        this.source = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (source != null) {
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Object> getSource() {
        return source;
    }
}

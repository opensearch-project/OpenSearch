/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.removeplugins;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for removing plugins
 *
 * @opensearch.internal
 */
public class RemovePluginsResponse extends ActionResponse implements ToXContentObject {
    private boolean success;
    private String message;
    private List<String> removedPlugins;

    public RemovePluginsResponse() {}

    public RemovePluginsResponse(StreamInput in) throws IOException {
        super(in);
        success = in.readBoolean();
        message = in.readOptionalString();
        removedPlugins = in.readStringList();
    }

    public RemovePluginsResponse(boolean success, String message, List<String> removedPlugins) {
        this.success = success;
        this.message = message;
        this.removedPlugins = removedPlugins;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeOptionalString(message);
        out.writeStringCollection(removedPlugins);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("success", success);
        if (message != null) {
            builder.field("message", message);
        }
        builder.field("removed_plugins", removedPlugins);
        builder.endObject();
        return builder;
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<String> getRemovedPlugins() { return removedPlugins; }
}

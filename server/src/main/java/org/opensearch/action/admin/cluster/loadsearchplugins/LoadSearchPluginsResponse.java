/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.loadsearchplugins;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for loading search plugins
 *
 * @opensearch.internal
 */
public class LoadSearchPluginsResponse extends ActionResponse implements ToXContentObject {
    private boolean success;
    private String message;
    private List<String> loadedPlugins;

    public LoadSearchPluginsResponse() {}

    public LoadSearchPluginsResponse(StreamInput in) throws IOException {
        super(in);
        success = in.readBoolean();
        message = in.readOptionalString();
        loadedPlugins = in.readStringList();
    }

    public LoadSearchPluginsResponse(boolean success, String message, List<String> loadedPlugins) {
        this.success = success;
        this.message = message;
        this.loadedPlugins = loadedPlugins;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeOptionalString(message);
        out.writeStringCollection(loadedPlugins);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("success", success);
        if (message != null) {
            builder.field("message", message);
        }
        builder.field("loaded_plugins", loadedPlugins);
        builder.endObject();
        return builder;
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<String> getLoadedPlugins() { return loadedPlugins; }
}

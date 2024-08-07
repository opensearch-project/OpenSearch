/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.executioncontextplugin;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response object containing the name of the plugin executing the transport action
 */
public class TestGetExecutionContextResponse extends ActionResponse implements ToXContentObject {
    private final String pluginClassName;

    /**
     * Default constructor
     *
     * @param pluginClassName the canonical class name of the plugin executing the transport action
     */
    public TestGetExecutionContextResponse(String pluginClassName) {
        this.pluginClassName = pluginClassName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pluginClassName);
    }

    /**
     * Constructor with StreamInput
     *
     * @param in the stream input
     */
    public TestGetExecutionContextResponse(final StreamInput in) throws IOException {
        pluginClassName = in.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("plugin_execution_context", pluginClassName);
        builder.endObject();
        return builder;
    }
}

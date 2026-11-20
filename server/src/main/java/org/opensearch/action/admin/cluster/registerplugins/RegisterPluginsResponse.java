/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.registerplugins;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for registering plugins
 *
 * @opensearch.internal
 */
public class RegisterPluginsResponse extends ActionResponse implements ToXContentObject {
    private boolean success;
    private String message;
    private List<String> registeredComponents;

    public RegisterPluginsResponse() {}

    public RegisterPluginsResponse(StreamInput in) throws IOException {
        super(in);
        success = in.readBoolean();
        message = in.readOptionalString();
        registeredComponents = in.readStringList();
    }

    public RegisterPluginsResponse(boolean success, String message, List<String> registeredComponents) {
        this.success = success;
        this.message = message;
        this.registeredComponents = registeredComponents;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeOptionalString(message);
        out.writeStringCollection(registeredComponents);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("success", success);
        if (message != null) {
            builder.field("message", message);
        }
        builder.field("registered_components", registeredComponents);
        builder.endObject();
        return builder;
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<String> getRegisteredComponents() { return registeredComponents; }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A request for get Resource Limit Group
 *
 * @opensearch.internal
 */
public class GetResourceLimitGroupRequest extends ActionRequest implements Writeable.Reader<GetResourceLimitGroupRequest> {
    String name;

    /**
     * Default constructor for GetResourceLimitGroupRequest
     * @param name - name for the Resource Limit Group to get
     */
    public GetResourceLimitGroupRequest(String name) {
        this.name = name;
    }

    /**
     * Constructor for GetResourceLimitGroupRequest
     * @param resourceLimitGroup - A {@link ResourceLimitGroup} object
     */
    public GetResourceLimitGroupRequest(ResourceLimitGroup resourceLimitGroup) {
        this.name = resourceLimitGroup.getName();
    }

    /**
     * Constructor for GetResourceLimitGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public GetResourceLimitGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }

    @Override
    public GetResourceLimitGroupRequest read(StreamInput in) throws IOException {
        return new GetResourceLimitGroupRequest(in);
    }

    /**
     * Generate a GetResourceLimitGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static GetResourceLimitGroupRequest fromXContent(XContentParser parser) throws IOException {
        ResourceLimitGroup resourceLimitGroup = ResourceLimitGroup.fromXContent(parser);
        return new GetResourceLimitGroupRequest(resourceLimitGroup);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Name getter
     */
    public String getName() {
        return name;
    }

    /**
     * Name setter
     * @param name - name to be set
     */
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
    }
}

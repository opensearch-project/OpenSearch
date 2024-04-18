/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for the update API for resource limit groups
 *
 * @opensearch.internal
 */
public class UpdateResourceLimitGroupResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final ResourceLimitGroup resourceLimitGroup;
    private RestStatus restStatus;

    /**
     * Constructor for UpdateResourceLimitGroupResponse
     */
    public UpdateResourceLimitGroupResponse() {
        this.resourceLimitGroup = null;
    }

    /**
     * Constructor for UpdateResourceLimitGroupResponse
     * @param resourceLimitGroup - The resource limit group to be updated
     */
    public UpdateResourceLimitGroupResponse(final ResourceLimitGroup resourceLimitGroup) {
        this.resourceLimitGroup = resourceLimitGroup;
    }

    /**
     * Constructor for UpdateResourceLimitGroupResponse
     * @param in - A {@link StreamInput} object
     */
    public UpdateResourceLimitGroupResponse(StreamInput in) throws IOException {
        resourceLimitGroup = new ResourceLimitGroup(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        resourceLimitGroup.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        resourceLimitGroup.toXContent(builder, params);
        return builder;
    }

    /**
     * resourceLimitGroup getter
     */
    public ResourceLimitGroup getResourceLimitGroup() {
        return resourceLimitGroup;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }

    /**
     * restStatus setter
     * @param restStatus - A {@link RestStatus} object
     */
    public void setRestStatus(RestStatus restStatus) {
        this.restStatus = restStatus;
    }
}

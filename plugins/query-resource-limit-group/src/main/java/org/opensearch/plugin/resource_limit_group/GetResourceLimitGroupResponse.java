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
import java.util.List;

/**
 * Response for the get API for resource limit groups
 *
 * @opensearch.internal
 */
public class GetResourceLimitGroupResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final List<ResourceLimitGroup> resourceLimitGroups;
    private RestStatus restStatus;

    /**
     * Constructor for GetResourceLimitGroupResponse
     */
    public GetResourceLimitGroupResponse() {
        this.resourceLimitGroups = null;
    }

    /**
     * Constructor for GetResourceLimitGroupResponse
     * @param resourceLimitGroups - The resource limit group list to be fetched
     */
    public GetResourceLimitGroupResponse(final List<ResourceLimitGroup> resourceLimitGroups) {
        this.resourceLimitGroups = resourceLimitGroups;
    }

    /**
     * Constructor for GetResourceLimitGroupResponse
     * @param in - A {@link StreamInput} object
     */
    public GetResourceLimitGroupResponse(StreamInput in) throws IOException {
        this.resourceLimitGroups = in.readList(ResourceLimitGroup::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(resourceLimitGroups);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("resource_limit_groups");
        for (ResourceLimitGroup rlg : resourceLimitGroups) {
            rlg.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * resourceLimitGroup getter
     */
    public List<ResourceLimitGroup> getResourceLimitGroups() {
        return resourceLimitGroups;
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

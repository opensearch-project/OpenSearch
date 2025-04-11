/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * A request for create WorkloadGroup
 * User input schema:
 *  {
 *    "name": "analytics",
 *    "resiliency_mode": "enforced",
 *    "resource_limits": {
 *           "cpu" : 0.4,
 *           "memory" : 0.2
 *      }
 *  }
 *
 * @opensearch.experimental
 */
public class CreateWorkloadGroupRequest extends ClusterManagerNodeRequest<CreateWorkloadGroupRequest> {
    private final WorkloadGroup workloadGroup;

    /**
     * Constructor for CreateWorkloadGroupRequest
     * @param workloadGroup - A {@link WorkloadGroup} object
     */
    CreateWorkloadGroupRequest(WorkloadGroup workloadGroup) {
        this.workloadGroup = workloadGroup;
    }

    /**
     * Constructor for CreateWorkloadGroupRequest
     * @param in - A {@link StreamInput} object
     */
    CreateWorkloadGroupRequest(StreamInput in) throws IOException {
        super(in);
        workloadGroup = new WorkloadGroup(in);
    }

    /**
     * Generate a CreateWorkloadGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static CreateWorkloadGroupRequest fromXContent(XContentParser parser) throws IOException {
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        return new CreateWorkloadGroupRequest(builder._id(UUIDs.randomBase64UUID()).updatedAt(Instant.now().getMillis()).build());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        workloadGroup.writeTo(out);
    }

    /**
     * WorkloadGroup getter
     */
    public WorkloadGroup getWorkloadGroup() {
        return workloadGroup;
    }
}

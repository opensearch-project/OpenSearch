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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
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
    /**
     * Why a create or update that sets {@code queue} fails: the field is reserved for request queueing, which this version
     * does not implement.
     */
    static final String QUEUEING_NOT_AVAILABLE = "request queueing ([queue]) is not available yet";

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
        // Before build(), which drops the difference between an absent queue and {} or null: any of them present is rejected.
        rejectQueueConfig(builder.getMutableWorkloadGroupFragment().getQueue() != null);
        return new CreateWorkloadGroupRequest(builder._id(UUIDs.randomBase64UUID()).updatedAt(Instant.now().getMillis()).build());
    }

    @Override
    public ActionRequestValidationException validate() {
        // Also runs on the cluster manager for a request another node forwarded, which may be on a version that accepts a
        // queue. The group is already normalized here, so only a non-empty queue is visible.
        rejectQueueConfig(workloadGroup.getMutableWorkloadGroupFragment().getQueue().isEmpty() == false);
        return null;
    }

    /**
     * Rejects a create or update request that sets the {@code queue} field, reserved for request queueing.
     * @param present whether the request sets {@code queue}
     */
    static void rejectQueueConfig(boolean present) {
        if (present) {
            throw new IllegalArgumentException(QUEUEING_NOT_AVAILABLE);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The fragment leaves both bags off the wire for an older node, so sending either would silently drop it.
        Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        Settings queue = workloadGroup.getMutableWorkloadGroupFragment().getQueue();
        if (out.getVersion().before(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)
            && ((throttling != null && throttling.isEmpty() == false) || (queue != null && queue.isEmpty() == false))) {
            throw new IllegalArgumentException(
                "cannot send workload group throttling or queue config to a node before version "
                    + WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION
            );
        }
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

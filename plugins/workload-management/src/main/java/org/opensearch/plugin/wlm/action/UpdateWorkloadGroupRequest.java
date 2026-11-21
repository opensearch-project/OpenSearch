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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;

import static org.opensearch.plugin.wlm.action.CreateWorkloadGroupRequest.rejectQueueConfig;

/**
 * A request for update WorkloadGroup
 *
 * @opensearch.experimental
 */
public class UpdateWorkloadGroupRequest extends ClusterManagerNodeRequest<UpdateWorkloadGroupRequest> {
    private final String name;
    private final MutableWorkloadGroupFragment mutableWorkloadGroupFragment;

    /**
     * Constructor for UpdateWorkloadGroupRequest
     * @param name - WorkloadGroup name for UpdateWorkloadGroupRequest
     * @param mutableWorkloadGroupFragment - MutableWorkloadGroupFragment for UpdateWorkloadGroupRequest
     */
    UpdateWorkloadGroupRequest(String name, MutableWorkloadGroupFragment mutableWorkloadGroupFragment) {
        this.name = name;
        this.mutableWorkloadGroupFragment = mutableWorkloadGroupFragment;
    }

    /**
     * Constructor for UpdateWorkloadGroupRequest
     * @param in - A {@link StreamInput} object
     */
    UpdateWorkloadGroupRequest(StreamInput in) throws IOException {
        // The superclass fields (parent task id, cluster_manager_timeout) are on the wire only from the throttling version:
        // older peers never wrote them. super(in) cannot be called conditionally, so this reads exactly what it would.
        if (in.getVersion().onOrAfter(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)) {
            setParentTask(TaskId.readFromStream(in));
            clusterManagerNodeTimeout = in.readTimeValue();
        }
        this.name = in.readString();
        this.mutableWorkloadGroupFragment = new MutableWorkloadGroupFragment(in);
    }

    /**
     * Generate a UpdateWorkloadGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the WorkloadGroup to be updated
     */
    public static UpdateWorkloadGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        rejectQueueConfig(builder.getMutableWorkloadGroupFragment().getQueue() != null);
        return new UpdateWorkloadGroupRequest(name, builder.getMutableWorkloadGroupFragment());
    }

    @Override
    public ActionRequestValidationException validate() {
        WorkloadGroup.validateName(name);
        // Also runs on the cluster manager for a request another node forwarded, which may be on a version that accepts a
        // queue. An absent queue stays null on the wire, while {} and null arrive as an empty bag, so any of them is seen.
        rejectQueueConfig(mutableWorkloadGroupFragment.getQueue() != null);
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * mutableWorkloadGroupFragment getter
     */
    public MutableWorkloadGroupFragment getmMutableWorkloadGroupFragment() {
        return mutableWorkloadGroupFragment;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The fragment leaves both bags off the wire for an older node, so sending either would silently drop it.
        Settings throttling = mutableWorkloadGroupFragment.getThrottling();
        Settings queue = mutableWorkloadGroupFragment.getQueue();
        if (out.getVersion().before(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)
            && ((throttling != null && throttling.isEmpty() == false) || (queue != null && queue.isEmpty() == false))) {
            throw new IllegalArgumentException(
                "cannot send workload group throttling or queue config to a node before version "
                    + WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION
            );
        }
        // Mirrors the StreamInput constructor: an older peer reads no superclass fields.
        if (out.getVersion().onOrAfter(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)) {
            super.writeTo(out);
        }
        out.writeString(name);
        mutableWorkloadGroupFragment.writeTo(out);
    }
}

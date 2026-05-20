/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for delete WorkloadGroup
 *
 * @opensearch.experimental
 */
public class DeleteWorkloadGroupRequest extends AcknowledgedRequest<DeleteWorkloadGroupRequest> {
    private final String name;

    /**
     * Default constructor for DeleteWorkloadGroupRequest
     * @param name - name for the WorkloadGroup to get
     */
    public DeleteWorkloadGroupRequest(String name) {
        this.name = name;
    }

    /**
     * Constructor for DeleteWorkloadGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public DeleteWorkloadGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (name == null) {
            ActionRequestValidationException actionRequestValidationException = new ActionRequestValidationException();
            actionRequestValidationException.addValidationError("WorkloadGroup name is missing");
            return actionRequestValidationException;
        }
        return null;
    }

    /**
     * Name getter
     */
    public String getName() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
    }
}

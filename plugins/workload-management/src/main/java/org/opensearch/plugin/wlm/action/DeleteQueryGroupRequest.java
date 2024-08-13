/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for delete QueryGroup
 *
 * @opensearch.experimental
 */
public class DeleteQueryGroupRequest extends ActionRequest {
    private final String name;

    /**
     * Default constructor for DeleteQueryGroupRequest
     * @param name - name for the QueryGroup to get
     */
    public DeleteQueryGroupRequest(String name) {
        this.name = name;
    }

    /**
     * Constructor for DeleteQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public DeleteQueryGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (name == null) {
            ActionRequestValidationException actionRequestValidationException = new ActionRequestValidationException();
            actionRequestValidationException.addValidationError("QueryGroup name is missing");
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

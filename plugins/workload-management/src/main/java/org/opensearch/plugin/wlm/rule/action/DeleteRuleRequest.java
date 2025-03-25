/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to delete a Rule by id.
 * @opensearch.experimental
 */
public class DeleteRuleRequest extends ActionRequest {
    private final String id;

    public DeleteRuleRequest(String id) {
        this.id = id;
    }

    public DeleteRuleRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (id == null) {
            ActionRequestValidationException exception = new ActionRequestValidationException();
            exception.addValidationError("id is missing");
            return exception;
        }
        return null;
    }


    public String getId() {
        return id;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for coordinator-level query execution.
 * Carries a serialized RelNode as JSON (produced by {@code RelJsonWriter}).
 */
public class QueryRequest extends ActionRequest {

    private final String jsonPlan;

    public QueryRequest(String jsonPlan) {
        this.jsonPlan = jsonPlan != null ? jsonPlan : "";
    }

    public QueryRequest(StreamInput in) throws IOException {
        super(in);
        this.jsonPlan = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(jsonPlan);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getJsonPlan() {
        return jsonPlan;
    }
}

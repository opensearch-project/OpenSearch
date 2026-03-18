/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport-layer request carrying PPL query text for the unified PPL endpoint.
 */
public class PPLRequest extends ActionRequest {

    private final String pplText;

    public PPLRequest(String pplText) {
        this.pplText = pplText;
    }

    public PPLRequest(StreamInput in) throws IOException {
        super(in);
        this.pplText = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pplText);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pplText == null || pplText.isEmpty()) {
            validationException = addValidationError("pplText is missing or empty", validationException);
        }
        return validationException;
    }

    public String getPplText() {
        return pplText;
    }
}

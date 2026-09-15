/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.Version;
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
    private final boolean explain;
    private final Integer targetPartitions;

    public PPLRequest(String pplText) {
        this(pplText, false, null);
    }

    public PPLRequest(String pplText, boolean explain) {
        this(pplText, explain, null);
    }

    public PPLRequest(String pplText, boolean explain, Integer targetPartitions) {
        this.pplText = pplText;
        this.explain = explain;
        this.targetPartitions = targetPartitions;
    }

    public PPLRequest(StreamInput in) throws IOException {
        super(in);
        this.pplText = in.readString();
        this.explain = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_3_9_0)) {
            this.targetPartitions = in.readOptionalVInt();
        } else {
            this.targetPartitions = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pplText);
        out.writeBoolean(explain);
        if (out.getVersion().onOrAfter(Version.V_3_9_0)) {
            out.writeOptionalVInt(targetPartitions);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pplText == null || pplText.isEmpty()) {
            validationException = addValidationError("pplText is missing or empty", validationException);
        }
        if (targetPartitions != null && targetPartitions < 1) {
            validationException = addValidationError("targetPartitions must be >= 1", validationException);
        }
        return validationException;
    }

    public String getPplText() {
        return pplText;
    }

    public boolean isExplain() {
        return explain;
    }

    public Integer getTargetPartitions() {
        return targetPartitions;
    }
}

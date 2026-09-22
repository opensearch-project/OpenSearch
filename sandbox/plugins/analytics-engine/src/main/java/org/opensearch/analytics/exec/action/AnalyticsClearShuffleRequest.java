/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instructs the receiving data node to release all hash-shuffle buffers it holds for {@code queryId}
 * ({@code ShuffleBufferManager.clearForQuery}). Broadcast by the coordinator on query terminal.
 *
 * @opensearch.internal
 */
public class AnalyticsClearShuffleRequest extends ActionRequest {

    private final String queryId;

    public AnalyticsClearShuffleRequest(String queryId) {
        this.queryId = queryId;
    }

    public AnalyticsClearShuffleRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
    }

    public String getQueryId() {
        return queryId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
    }
}

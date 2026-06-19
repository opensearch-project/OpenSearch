/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.model;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for a get tiering status request.
 */
public class GetTieringStatusResponse extends ActionResponse implements ToXContentObject {

    /** Returns the tiering status. */
    public TieringStatus getTieringStatus() {
        return tieringStatus;
    }

    private TieringStatus tieringStatus;

    /**
     * Constructs a response with the given tiering status.
     * @param tieringStatus the tiering status
     */
    public GetTieringStatusResponse(TieringStatus tieringStatus) {
        this.tieringStatus = tieringStatus;
    }

    /**
     * Constructs a response from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public GetTieringStatusResponse(StreamInput in) throws IOException {
        tieringStatus = TieringStatus.readFrom(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        tieringStatus.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        tieringStatus.writeTo(out);
    }
}

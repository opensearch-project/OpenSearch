/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to get current ingestion state when using pull-based ingestion. This request supports retrieving index and
 * shard level state. By default, all shards of an index are included.
 * Todo: pagination will be supported in the future.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class GetIngestionStateRequest extends BroadcastRequest<GetIngestionStateRequest> {

    private String index;

    private int[] shards;

    public GetIngestionStateRequest(String index) {
        super();
        this.index = index;
        this.shards = new int[] {};
    }

    public GetIngestionStateRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shards = in.readIntArray();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeIntArray(shards);
    }

    public String getIndex() {
        return index;
    }

    public int[] getShards() {
        return shards;
    }

    public void setShards(int[] shards) {
        this.shards = shards;
    }
}

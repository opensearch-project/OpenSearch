/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

public class PrepareShardResponse extends TransportResponse {

    public String getUuid() {
        return uuid;
    }

    private final String uuid;

    public PrepareShardResponse(String uuid) {
        this.uuid = uuid;
    }

    public PrepareShardResponse(StreamInput in) throws IOException {
        uuid = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uuid);
    }
}

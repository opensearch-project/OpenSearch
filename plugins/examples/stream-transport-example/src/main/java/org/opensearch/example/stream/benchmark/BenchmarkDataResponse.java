/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

class BenchmarkDataResponse extends TransportResponse {
    private final byte[] payload;

    BenchmarkDataResponse(byte[] payload) {
        this.payload = payload;
    }

    BenchmarkDataResponse(StreamInput in) throws IOException {
        super(in);
        this.payload = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(payload);
    }

    int getPayloadSize() {
        return payload != null ? payload.length : 0;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

public class GetFilesResponse extends TransportResponse {

    public GetFilesResponse(StreamInput in) {
    }

    public GetFilesResponse() {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}

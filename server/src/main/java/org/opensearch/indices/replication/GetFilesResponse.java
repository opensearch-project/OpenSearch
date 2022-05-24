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

/**
 * Response from a {@link SegmentReplicationSource} indicating that a replication event has completed.
 *
 * @opensearch.internal
 */
public class GetFilesResponse extends TransportResponse {

    public GetFilesResponse() {}

    public GetFilesResponse(StreamInput streamInput) {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}

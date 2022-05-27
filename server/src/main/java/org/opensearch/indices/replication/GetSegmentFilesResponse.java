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
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * Response from a {@link SegmentReplicationSource} indicating that a replication event has completed.
 *
 * @opensearch.internal
 */
public class GetSegmentFilesResponse extends TransportResponse {

    List<StoreFileMetadata> files;

    public GetSegmentFilesResponse(List<StoreFileMetadata> files) {
        this.files = files;
    }

    public GetSegmentFilesResponse(StreamInput out) throws IOException {
        out.readList(StoreFileMetadata::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(files);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class RemoteStorePublishMergedSegmentRequestTests extends OpenSearchTestCase {

    public void testPublishMergedSegmentRequest() {
        RemoteStoreMergedSegmentCheckpoint checkpoint = new RemoteStoreMergedSegmentCheckpoint(
            new MergedSegmentCheckpoint(new ShardId(new Index("1", "1"), 0), 0, 0, 0, "", Collections.emptyMap(), "_0"),
            Map.of("_0", "_0__uuid")
        );

        RemoteStorePublishMergedSegmentRequest request = new RemoteStorePublishMergedSegmentRequest(checkpoint);
        assertNull(request.validate());
        assertEquals(checkpoint, request.getMergedSegment());
        assertEquals(Objects.hash(checkpoint), request.hashCode());
        assertEquals(checkpoint.getLocalToRemoteSegmentFilenameMap(), Map.of("_0", "_0__uuid"));
    }

    public void testSerialize() throws Exception {
        RemoteStoreMergedSegmentCheckpoint checkpoint = new RemoteStoreMergedSegmentCheckpoint(
            new MergedSegmentCheckpoint(new ShardId(new Index("1", "1"), 0), 0, 0, 0, "", Collections.emptyMap(), "_0"),
            Map.of("_0", "_0__uuid")
        );
        RemoteStorePublishMergedSegmentRequest originalRequest = new RemoteStorePublishMergedSegmentRequest(checkpoint);
        RemoteStorePublishMergedSegmentRequest cloneRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest = new RemoteStorePublishMergedSegmentRequest(in);
            }
        }
        assertEquals(cloneRequest.getMergedSegment(), originalRequest.getMergedSegment());
    }
}

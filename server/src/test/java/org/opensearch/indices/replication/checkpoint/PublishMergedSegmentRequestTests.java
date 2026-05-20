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
import java.util.Objects;

import static org.hamcrest.core.IsEqual.equalTo;

public class PublishMergedSegmentRequestTests extends OpenSearchTestCase {

    public void testPublishMergedSegmentRequest() {
        MergedSegmentCheckpoint checkpoint = new MergedSegmentCheckpoint(
            new ShardId(new Index("1", "1"), 0),
            0,
            1,
            0,
            "",
            Collections.emptyMap(),
            "_0"
        );
        PublishMergedSegmentRequest request = new PublishMergedSegmentRequest(checkpoint);
        assertNull(request.validate());
        assertEquals(checkpoint, request.getMergedSegment());
        assertEquals(Objects.hash(checkpoint), request.hashCode());
    }

    public void testSerialize() throws Exception {
        MergedSegmentCheckpoint checkpoint = new MergedSegmentCheckpoint(
            new ShardId(new Index("1", "1"), 0),
            0,
            1,
            0,
            "",
            Collections.emptyMap(),
            "_0"
        );
        PublishMergedSegmentRequest originalRequest = new PublishMergedSegmentRequest(checkpoint);
        PublishMergedSegmentRequest cloneRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest = new PublishMergedSegmentRequest(in);
            }
        }
        assertThat(cloneRequest, equalTo(originalRequest));
        assertThat(cloneRequest.getMergedSegment(), equalTo(originalRequest.getMergedSegment()));
    }
}

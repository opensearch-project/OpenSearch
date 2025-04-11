/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

public class GetIngestionStateResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        ShardIngestionState[] shardStates = new ShardIngestionState[] {
            new ShardIngestionState("index1", 0, "POLLING", "DROP", false),
            new ShardIngestionState("index1", 1, "PAUSED", "BLOCK", true) };
        GetIngestionStateResponse response = new GetIngestionStateResponse(shardStates, 2, 2, 0, null, Collections.emptyList());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                GetIngestionStateResponse deserializedResponse = new GetIngestionStateResponse(in);
                assertEquals(response.getShardStates()[0].shardId(), deserializedResponse.getShardStates()[0].shardId());
                assertEquals(response.getShardStates()[1].shardId(), deserializedResponse.getShardStates()[1].shardId());
                assertEquals(response.getTotalShards(), deserializedResponse.getTotalShards());
                assertEquals(response.getSuccessfulShards(), deserializedResponse.getSuccessfulShards());
                assertEquals(response.getFailedShards(), deserializedResponse.getFailedShards());
            }
        }
    }
}

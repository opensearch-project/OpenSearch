/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IngestionUpdateStateResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        IngestionStateShardFailure[] shardFailures = new IngestionStateShardFailure[] {
            new IngestionStateShardFailure("index1", 0, "test failure") };
        IngestionUpdateStateResponse response = new IngestionUpdateStateResponse(true, true, shardFailures, "test error");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                IngestionUpdateStateResponse deserializedResponse = new IngestionUpdateStateResponse(in);
                assertTrue(deserializedResponse.isAcknowledged());
                assertTrue(deserializedResponse.isShardsAcknowledged());
                assertNotNull(deserializedResponse.getShardFailures());
                assertEquals(1, deserializedResponse.getShardFailures().length);
                assertEquals("index1", deserializedResponse.getShardFailures()[0].index());
                assertEquals(0, deserializedResponse.getShardFailures()[0].shard());
                assertEquals("test error", deserializedResponse.getErrorMessage());
            }
        }
    }

    public void testShardFailureGrouping() {
        IngestionStateShardFailure[] shardFailures = new IngestionStateShardFailure[] {
            new IngestionStateShardFailure("index1", 0, "failure 1"),
            new IngestionStateShardFailure("index1", 1, "failure 2"),
            new IngestionStateShardFailure("index2", 0, "failure 3") };
        IngestionUpdateStateResponse response = new IngestionUpdateStateResponse(true, true, shardFailures, "test error");

        Map<String, List<IngestionStateShardFailure>> groupedFailures = IngestionStateShardFailure.groupShardFailuresByIndex(shardFailures);
        assertEquals(2, groupedFailures.size());
        assertEquals(2, groupedFailures.get("index1").size());
        assertEquals(1, groupedFailures.get("index2").size());
        assertEquals(0, groupedFailures.get("index1").get(0).shard());
        assertEquals(1, groupedFailures.get("index1").get(1).shard());
        assertEquals(0, groupedFailures.get("index2").get(0).shard());
    }
}

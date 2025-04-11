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
import java.util.List;
import java.util.Map;

public class ShardIngestionStateTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        ShardIngestionState state = new ShardIngestionState("index1", 0, "POLLING", "DROP", false);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            state.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                ShardIngestionState deserializedState = new ShardIngestionState(in);
                assertEquals(state.index(), deserializedState.index());
                assertEquals(state.shardId(), deserializedState.shardId());
                assertEquals(state.pollerState(), deserializedState.pollerState());
                assertEquals(state.isPollerPaused(), deserializedState.isPollerPaused());
            }
        }
    }

    public void testSerializationWithNullValues() throws IOException {
        ShardIngestionState state = new ShardIngestionState("index1", 0, null, null, false);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            state.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                ShardIngestionState deserializedState = new ShardIngestionState(in);
                assertEquals(state.index(), deserializedState.index());
                assertEquals(state.shardId(), deserializedState.shardId());
                assertNull(deserializedState.pollerState());
                assertEquals(state.isPollerPaused(), deserializedState.isPollerPaused());
            }
        }
    }

    public void testGroupShardStateByIndex() {
        ShardIngestionState[] states = new ShardIngestionState[] {
            new ShardIngestionState("index1", 0, "POLLING", "DROP", true),
            new ShardIngestionState("index1", 1, "PAUSED", "DROP", false),
            new ShardIngestionState("index2", 0, "POLLING", "DROP", true) };

        Map<String, List<ShardIngestionState>> groupedStates = ShardIngestionState.groupShardStateByIndex(states);

        assertEquals(2, groupedStates.size());
        assertEquals(2, groupedStates.get("index1").size());
        assertEquals(1, groupedStates.get("index2").size());

        // Verify index1 shards
        List<ShardIngestionState> indexStates1 = groupedStates.get("index1");
        assertEquals(0, indexStates1.get(0).shardId());
        assertEquals(1, indexStates1.get(1).shardId());

        // Verify index2 shards
        List<ShardIngestionState> indexStates2 = groupedStates.get("index2");
        assertEquals(0, indexStates2.get(0).shardId());
    }
}

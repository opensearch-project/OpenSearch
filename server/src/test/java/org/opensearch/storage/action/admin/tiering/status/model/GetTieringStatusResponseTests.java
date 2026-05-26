/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.model;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetTieringStatusResponseTests extends OpenSearchTestCase {

    public void testXContent() throws IOException {
        TieringStatus status = new TieringStatus("test-index", "RUNNING", "hot", "warm", 123);

        final GetTieringStatusResponse response = new GetTieringStatusResponse(status);

        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));

        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(TieringStatus.TIERING_STATUS, parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken();
            // assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            final String currentFieldName = parser.currentName();
            if (currentFieldName.equals(TieringStatus.INDEX)) {
                assertEquals("test-index", parser.text());
            } else if (currentFieldName.equals(TieringStatus.STATE)) {
                assertEquals("RUNNING", parser.text());
            } else if (currentFieldName.equals(TieringStatus.SOURCE)) {
                assertEquals("hot", parser.text());
            } else if (currentFieldName.equals(TieringStatus.TARGET)) {
                assertEquals("warm", parser.text());
            } else if (currentFieldName.equals(TieringStatus.START_TIME)) {
                assertEquals(123, parser.longValue());
            }
        }
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
    }

    public void testStreamSerialization_withRelocatingShards() throws IOException {
        // Create original object
        TieringStatus originalStatus = new TieringStatus("test-index", "RUNNING", "hot", "warm", 123L);
        Map<String, Integer> shardCounters = new HashMap<>();
        shardCounters.put(TieringStatus.PENDING_SHARDS, 2);
        shardCounters.put(TieringStatus.SUCCEEDED_SHARDS, 3);
        shardCounters.put(TieringStatus.RUNNING_SHARDS, 1);
        shardCounters.put(TieringStatus.TOTAL_SHARDS, 6);
        List<TieringStatus.OngoingShard> ongoingShards = Collections.singletonList(new TieringStatus.OngoingShard(1, "node1"));
        originalStatus.setShardLevelStatus(new TieringStatus.ShardLevelStatus(shardCounters, ongoingShards));

        // Serialize and deserialize
        BytesStreamOutput out = new BytesStreamOutput();
        originalStatus.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TieringStatus deserializedStatus = TieringStatus.readFrom(in);

        // Verify deserialized object
        assertEquals(originalStatus.getIndexName(), deserializedStatus.getIndexName());
        assertEquals(originalStatus.getStatus(), deserializedStatus.getStatus());
        assertEquals(originalStatus.getSource(), deserializedStatus.getSource());
        assertEquals(originalStatus.getTarget(), deserializedStatus.getTarget());
        assertEquals(originalStatus.getStartTime(), deserializedStatus.getStartTime());

        assertNotNull(deserializedStatus.getShardLevelStatus());
        assertEquals(2, deserializedStatus.getShardLevelStatus().getShardLevelCounters().get(TieringStatus.PENDING_SHARDS).intValue());
        assertEquals(3, deserializedStatus.getShardLevelStatus().getShardLevelCounters().get(TieringStatus.SUCCEEDED_SHARDS).intValue());
    }

    public void testStreamSerialization_withNoRelocatingShards() throws IOException {
        // Create original object
        TieringStatus originalStatus = new TieringStatus("test-index", "RUNNING", "hot", "warm", 123L);
        Map<String, Integer> shardCounters = new HashMap<>();
        shardCounters.put(TieringStatus.PENDING_SHARDS, 2);
        shardCounters.put(TieringStatus.SUCCEEDED_SHARDS, 3);
        shardCounters.put(TieringStatus.RUNNING_SHARDS, 1);
        shardCounters.put(TieringStatus.TOTAL_SHARDS, 6);

        originalStatus.setShardLevelStatus(new TieringStatus.ShardLevelStatus(shardCounters, new ArrayList<>()));

        // Serialize and deserialize
        BytesStreamOutput out = new BytesStreamOutput();
        originalStatus.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TieringStatus deserializedStatus = TieringStatus.readFrom(in);

        // Verify deserialized object
        assertEquals(originalStatus.getIndexName(), deserializedStatus.getIndexName());
        assertEquals(originalStatus.getStatus(), deserializedStatus.getStatus());
        assertEquals(originalStatus.getSource(), deserializedStatus.getSource());
        assertEquals(originalStatus.getTarget(), deserializedStatus.getTarget());
        assertEquals(originalStatus.getStartTime(), deserializedStatus.getStartTime());

        assertNotNull(deserializedStatus.getShardLevelStatus());
        assertEquals(2, deserializedStatus.getShardLevelStatus().getShardLevelCounters().get(TieringStatus.PENDING_SHARDS).intValue());
        assertEquals(3, deserializedStatus.getShardLevelStatus().getShardLevelCounters().get(TieringStatus.SUCCEEDED_SHARDS).intValue());
    }

    public void testResponseStreamSerialization() throws IOException {
        TieringStatus status = new TieringStatus("test-index", "RUNNING", "hot", "warm", 123L);
        GetTieringStatusResponse original = new GetTieringStatusResponse(status);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        GetTieringStatusResponse deserialized = new GetTieringStatusResponse(in);

        assertEquals(original.getTieringStatus().getIndexName(), deserialized.getTieringStatus().getIndexName());
        assertEquals(original.getTieringStatus().getStatus(), deserialized.getTieringStatus().getStatus());
    }

    public void testTieringStatusSerialization() throws IOException {
        // Create sample data
        String indexName = "test-index";
        String state = "RUNNING";
        String source = "hot";
        String target = "warm";
        long startTime = 123L;

        Map<String, Integer> shardCounters = new HashMap<>();
        shardCounters.put(TieringStatus.PENDING_SHARDS, 2);
        shardCounters.put(TieringStatus.SUCCEEDED_SHARDS, 3);
        shardCounters.put(TieringStatus.RUNNING_SHARDS, 1);
        shardCounters.put(TieringStatus.TOTAL_SHARDS, 6);

        List<TieringStatus.OngoingShard> ongoingShards = Arrays.asList(
            new TieringStatus.OngoingShard(1, "node1"),
            new TieringStatus.OngoingShard(2, "node2")
        );

        TieringStatus.ShardLevelStatus shardStatus = new TieringStatus.ShardLevelStatus(shardCounters, ongoingShards);

        TieringStatus status = new TieringStatus(indexName, state, source, target, startTime);
        status.setShardLevelStatus(shardStatus);

        final GetTieringStatusResponse response = new GetTieringStatusResponse(status);

        // Test XContent serialization
        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));

        // Verify the parsed content
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(TieringStatus.TIERING_STATUS, parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case TieringStatus.INDEX:
                    assertEquals("test-index", parser.text());
                    break;
                case TieringStatus.STATE:
                    assertEquals("RUNNING", parser.text());
                    break;
                case TieringStatus.SOURCE:
                    assertEquals("hot", parser.text());
                    break;
                case TieringStatus.TARGET:
                    assertEquals("warm", parser.text());
                    break;
                case TieringStatus.START_TIME:
                    assertEquals(123L, parser.longValue());
                    break;
                case TieringStatus.SHARD_LEVEL_STATUS:
                    assertEquals(XContentParser.Token.START_OBJECT, parser.currentToken());

                    // Parse shard level counters
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String shardField = parser.currentName();
                        parser.nextToken();

                        switch (shardField) {
                            case TieringStatus.PENDING_SHARDS:
                                assertEquals(2, parser.intValue());
                                break;
                            case TieringStatus.SUCCEEDED_SHARDS:
                                assertEquals(3, parser.intValue());
                                break;
                            case TieringStatus.RUNNING_SHARDS:
                                assertEquals(1, parser.intValue());
                                break;
                            case TieringStatus.TOTAL_SHARDS:
                                assertEquals(6, parser.intValue());
                                break;
                            case "shard_relocation_status":
                                assertEquals(XContentParser.Token.START_ARRAY, parser.currentToken());

                                // First ongoing shard
                                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                                assertEquals("source_shard_id", parser.currentName());
                                assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
                                assertEquals(1, parser.intValue());
                                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                                assertEquals("relocating_node_id", parser.currentName());
                                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                                assertEquals("node1", parser.text());
                                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());

                                // Second ongoing shard
                                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                                assertEquals("source_shard_id", parser.currentName());
                                assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
                                assertEquals(2, parser.intValue());
                                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                                assertEquals("relocating_node_id", parser.currentName());
                                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                                assertEquals("node2", parser.text());
                                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());

                                assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());
                                break;
                        }
                    }
                    break;
            }
        }
    }
}

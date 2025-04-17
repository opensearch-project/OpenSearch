/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ScaleIndexResponseTests extends OpenSearchTestCase {

    private DiscoveryNode createTestNode() throws Exception {
        return new DiscoveryNode(
            "test_node",
            "test_node_id",
            new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    public void testSuccessfulResponse() throws Exception {
        // Create test node and responses with no failures
        DiscoveryNode node = createTestNode();
        List<ScaleIndexShardResponse> shardResponses = new ArrayList<>();

        // Add successful shard responses
        shardResponses.add(
            new ScaleIndexShardResponse(
                new ShardId(new Index("test_index", "test_uuid"), 0),
                false,  // doesn't need sync
                0      // no uncommitted operations
            )
        );

        List<ScaleIndexNodeResponse> nodeResponses = Collections.singletonList(new ScaleIndexNodeResponse(node, shardResponses));

        ScaleIndexResponse response = new ScaleIndexResponse(nodeResponses);

        // Verify response state
        assertFalse("Response should not have failures", response.hasFailures());
        assertNull("Failure reason should be null", response.buildFailureReason());
    }

    public void testResponseWithFailures() throws Exception {
        DiscoveryNode node = createTestNode();
        List<ScaleIndexShardResponse> shardResponses = new ArrayList<>();

        // Create an Index instance
        Index index = new Index("test_index", "test_uuid");

        // Add a failed shard response (needs sync)
        ShardId shardId0 = new ShardId(index, 0);
        shardResponses.add(
            new ScaleIndexShardResponse(
                shardId0,
                true,   // needs sync
                0       // no uncommitted operations
            )
        );

        // Add another failed shard response (has uncommitted operations)
        ShardId shardId1 = new ShardId(index, 1);
        shardResponses.add(
            new ScaleIndexShardResponse(
                shardId1,
                false,  // doesn't need sync
                5       // has uncommitted operations
            )
        );

        List<ScaleIndexNodeResponse> nodeResponses = Collections.singletonList(new ScaleIndexNodeResponse(node, shardResponses));

        ScaleIndexResponse response = new ScaleIndexResponse(nodeResponses);

        // Verify response state
        assertTrue("Response should have failures", response.hasFailures());
        assertNotNull("Failure reason should not be null", response.buildFailureReason());
        String failureReason = response.buildFailureReason();

        // Verify the exact shard IDs appear in the failure reason
        assertTrue("Failure reason should mention shard 0", failureReason.contains("Shard " + shardId0));
        assertTrue("Failure reason should mention shard 1", failureReason.contains("Shard " + shardId1));
        assertTrue("Failure reason should mention sync needed", failureReason.contains("needs sync"));
        assertTrue("Failure reason should mention uncommitted operations", failureReason.contains("has uncommitted operations"));
    }

    public void testSerialization() throws Exception {
        DiscoveryNode node = createTestNode();
        List<ScaleIndexShardResponse> shardResponses = new ArrayList<>();

        // Add mixed success/failure responses
        shardResponses.add(
            new ScaleIndexShardResponse(
                new ShardId(new Index("test_index", "test_uuid"), 0),
                false,  // doesn't need sync
                0       // no uncommitted operations
            )
        );
        shardResponses.add(
            new ScaleIndexShardResponse(
                new ShardId(new Index("test_index", "test_uuid"), 1),
                true,   // needs sync
                3       // has uncommitted operations
            )
        );

        List<ScaleIndexNodeResponse> nodeResponses = Collections.singletonList(new ScaleIndexNodeResponse(node, shardResponses));

        ScaleIndexResponse originalResponse = new ScaleIndexResponse(nodeResponses);

        // Serialize
        BytesStreamOutput output = new BytesStreamOutput();
        originalResponse.writeTo(output);

        // Deserialize - first read the node responses
        StreamInput input = output.bytes().streamInput();
        List<ScaleIndexNodeResponse> deserializedNodeResponses = input.readList(ScaleIndexNodeResponse::new);
        ScaleIndexResponse deserializedResponse = new ScaleIndexResponse(deserializedNodeResponses);

        // Verify serialization preserved state
        assertEquals("Failure state should match after serialization", originalResponse.hasFailures(), deserializedResponse.hasFailures());
        assertEquals(
            "Failure reason should match after serialization",
            originalResponse.buildFailureReason(),
            deserializedResponse.buildFailureReason()
        );
    }

    public void testToXContent() throws Exception {
        DiscoveryNode node = createTestNode();
        List<ScaleIndexShardResponse> shardResponses = new ArrayList<>();

        // Add a failed shard response
        shardResponses.add(
            new ScaleIndexShardResponse(
                new ShardId(new Index("test_index", "test_uuid"), 0),
                true,   // needs sync
                2       // has uncommitted operations
            )
        );

        List<ScaleIndexNodeResponse> nodeResponses = Collections.singletonList(new ScaleIndexNodeResponse(node, shardResponses));

        ScaleIndexResponse response = new ScaleIndexResponse(nodeResponses);

        // Convert to XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        String json = builder.toString();

        // Verify XContent output contains only the fields defined in toXContent()
        assertTrue("XContent should contain failure_reason field", json.contains("\"failure_reason\""));
        // The failure reason will contain details about the failure
        assertTrue("XContent should contain failure details", json.contains("Shard") && json.contains("needs sync"));
    }

    public void testEmptyResponse() throws Exception {
        // Create response with empty node responses
        ScaleIndexResponse response = new ScaleIndexResponse(Collections.emptyList());

        // Verify empty response state
        assertFalse("Empty response should not have failures", response.hasFailures());
        assertNull("Empty response should have null failure reason", response.buildFailureReason());
    }

    public void testMultiNodeResponse() throws Exception {
        List<ScaleIndexNodeResponse> nodeResponses = new ArrayList<>();

        // Create two nodes
        DiscoveryNode node1 = createTestNode();
        DiscoveryNode node2 = new DiscoveryNode(
            "test_node2",
            "test_node_id2",
            new TransportAddress(InetAddress.getByName("127.0.0.2"), 9300),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        // Create index and shards
        Index index = new Index("test_index", "test_uuid");
        ShardId shardId0 = new ShardId(index, 0);
        ShardId shardId1 = new ShardId(index, 1);

        // Add responses from both nodes
        List<ScaleIndexShardResponse> shardResponses1 = Collections.singletonList(
            new ScaleIndexShardResponse(
                shardId0,
                false,  // doesn't need sync
                0       // no uncommitted operations
            )
        );

        List<ScaleIndexShardResponse> shardResponses2 = Collections.singletonList(
            new ScaleIndexShardResponse(
                shardId1,
                true,   // needs sync
                0       // no uncommitted operations
            )
        );

        nodeResponses.add(new ScaleIndexNodeResponse(node1, shardResponses1));
        nodeResponses.add(new ScaleIndexNodeResponse(node2, shardResponses2));

        ScaleIndexResponse response = new ScaleIndexResponse(nodeResponses);

        // Verify multi-node response
        assertTrue("Response should have failures due to node2", response.hasFailures());
        String failureReason = response.buildFailureReason();
        assertTrue("Failure reason should mention node2", failureReason.contains("test_node2"));
        assertTrue("Failure reason should mention shard 1", failureReason.contains("Shard " + shardId1));
    }
}

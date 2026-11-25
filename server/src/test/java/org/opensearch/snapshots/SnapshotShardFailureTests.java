/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class SnapshotShardFailureTests extends OpenSearchTestCase {

    public void testToXContentWritesIndexUuid() throws IOException {
        ShardId shardId = new ShardId("my-index", "my-index-uuid", 3);
        SnapshotShardFailure failure = new SnapshotShardFailure("node-id", shardId, "reason");

        Map<String, Object> map = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            Strings.toString(MediaTypeRegistry.JSON, failure),
            false
        );

        assertEquals("my-index", map.get("index"));
        assertEquals("my-index-uuid", map.get("index_uuid"));
    }

    public void testFromXContentRoundTripPreservesIndexUuid() throws IOException {
        ShardId shardId = new ShardId("my-index", "my-index-uuid", 3);
        SnapshotShardFailure failure = new SnapshotShardFailure("node-id", shardId, "reason");

        String json = Strings.toString(MediaTypeRegistry.JSON, failure);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            SnapshotShardFailure parsed = SnapshotShardFailure.fromXContent(parser);
            assertEquals(json, Strings.toString(MediaTypeRegistry.JSON, parsed));
        }
    }

    public void testEqualsComparesIndexName() {
        SnapshotShardFailure failure = new SnapshotShardFailure("node-id", new ShardId("index-a", "uuid", 0), "reason");
        SnapshotShardFailure otherIndex = new SnapshotShardFailure("node-id", new ShardId("index-b", "uuid", 0), "reason");

        assertNotEquals(failure, otherIndex);
    }
}

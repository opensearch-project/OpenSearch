/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

public class PublishEntryTests extends OpenSearchTestCase {

    private static ShardId shard(String indexName, String uuid, int id) {
        return new ShardId(new Index(indexName, uuid), id);
    }

    private static PublishEntry minimalEntry() {
        return PublishEntry.builder()
            .publishId("pub-1")
            .indexName("logs-2024")
            .indexUUID("uuid-abc")
            .startedAt(12345L)
            .build();
    }

    public void testConstructorRejectsBlankPublishId() {
        expectThrows(
            IllegalArgumentException.class,
            () -> PublishEntry.builder().publishId("").indexName("i").indexUUID("u").startedAt(1L).build()
        );
    }

    public void testConstructorRejectsBlankIndexName() {
        expectThrows(
            IllegalArgumentException.class,
            () -> PublishEntry.builder().publishId("p").indexName("").indexUUID("u").startedAt(1L).build()
        );
    }

    public void testConstructorRejectsBlankIndexUUID() {
        expectThrows(
            IllegalArgumentException.class,
            () -> PublishEntry.builder().publishId("p").indexName("i").indexUUID("").startedAt(1L).build()
        );
    }

    public void testConstructorRejectsNonPositiveStartedAt() {
        expectThrows(
            IllegalArgumentException.class,
            () -> PublishEntry.builder().publishId("p").indexName("i").indexUUID("u").startedAt(0L).build()
        );
    }

    public void testConstructorRejectsNegativeRetryCount() {
        expectThrows(
            IllegalArgumentException.class,
            () -> PublishEntry.builder().publishId("p").indexName("i").indexUUID("u").startedAt(1L).retryCount(-1).build()
        );
    }

    public void testDefaultsAreReasonable() {
        PublishEntry e = minimalEntry();
        assertEquals(PublishPhase.INITIALIZED, e.phase());
        assertTrue(e.shardStatuses().isEmpty());
        assertNull(e.savedSnapshotId());
        assertEquals(0, e.retryCount());
        assertNull(e.lastFailureReason());
    }

    public void testWithPhase() {
        PublishEntry base = minimalEntry();
        PublishEntry advanced = base.withPhase(PublishPhase.PUBLISHING);
        assertEquals(PublishPhase.INITIALIZED, base.phase());
        assertEquals(PublishPhase.PUBLISHING, advanced.phase());
        assertEquals(base.publishId(), advanced.publishId());
    }

    public void testWithShardStatusesIsImmutableView() {
        Map<ShardId, PerShardStatus> statuses = new LinkedHashMap<>();
        statuses.put(shard("logs", "uuid-abc", 0), PerShardStatus.pending("node-1"));
        PublishEntry e = PublishEntry.builder()
            .publishId("p")
            .indexName("i")
            .indexUUID("u")
            .startedAt(1L)
            .shardStatuses(statuses)
            .build();
        // Defensive-copy guarantee: mutating the caller's map must not affect the entry.
        statuses.clear();
        assertEquals(1, e.shardStatuses().size());
        expectThrows(UnsupportedOperationException.class, () -> e.shardStatuses().clear());
    }

    public void testWithSavedSnapshotId() {
        PublishEntry after = minimalEntry().withSavedSnapshotId("snap-42");
        assertEquals("snap-42", after.savedSnapshotId());
    }

    public void testWithIncrementedRetryCount() {
        PublishEntry base = minimalEntry();
        assertEquals(0, base.retryCount());
        assertEquals(1, base.withIncrementedRetryCount().retryCount());
        assertEquals(2, base.withIncrementedRetryCount().withIncrementedRetryCount().retryCount());
    }

    public void testWithFailureReason() {
        PublishEntry after = minimalEntry().withFailureReason("shard 3 network error");
        assertEquals("shard 3 network error", after.lastFailureReason());
    }

    public void testWriteableRoundTripMinimal() throws Exception {
        PublishEntry original = minimalEntry();
        PublishEntry copy = serde(original);
        assertEquals(original, copy);
    }

    public void testWriteableRoundTripFullyPopulated() throws Exception {
        Map<ShardId, PerShardStatus> statuses = new LinkedHashMap<>();
        statuses.put(shard("logs", "uuid-abc", 0), PerShardStatus.pending("node-1"));
        statuses.put(shard("logs", "uuid-abc", 1), new PerShardStatus(PerShardStatus.State.SUCCESS, "node-2", null));
        statuses.put(shard("logs", "uuid-abc", 2), new PerShardStatus(PerShardStatus.State.FAILED, "node-3", "boom"));

        PublishEntry original = PublishEntry.builder()
            .publishId("pub-xyz")
            .indexName("logs")
            .indexUUID("uuid-abc")
            .phase(PublishPhase.PUBLISHING)
            .shardStatuses(statuses)
            .savedSnapshotId("snap-7")
            .startedAt(99999L)
            .retryCount(3)
            .lastFailureReason("shard 2 failed")
            .build();

        PublishEntry copy = serde(original);
        assertEquals(original, copy);
        assertEquals(original.shardStatuses(), copy.shardStatuses());
    }

    public void testToXContentContainsExpectedFields() throws Exception {
        Map<ShardId, PerShardStatus> statuses = new LinkedHashMap<>();
        statuses.put(shard("logs", "uuid-abc", 0), PerShardStatus.pending("node-1"));

        PublishEntry entry = PublishEntry.builder()
            .publishId("pub-xyz")
            .indexName("logs")
            .indexUUID("uuid-abc")
            .phase(PublishPhase.PUBLISHING)
            .shardStatuses(statuses)
            .savedSnapshotId("snap-7")
            .startedAt(99999L)
            .retryCount(2)
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json, json.contains("\"publish_id\":\"pub-xyz\""));
        assertTrue(json, json.contains("\"index_name\":\"logs\""));
        assertTrue(json, json.contains("\"index_uuid\":\"uuid-abc\""));
        assertTrue(json, json.contains("\"phase\":\"PUBLISHING\""));
        assertTrue(json, json.contains("\"started_at_millis\":99999"));
        assertTrue(json, json.contains("\"retry_count\":2"));
        assertTrue(json, json.contains("\"saved_snapshot_id\":\"snap-7\""));
        assertTrue(json, json.contains("\"shards\""));
    }

    private static PublishEntry serde(PublishEntry original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new PublishEntry(in);
            }
        }
    }
}

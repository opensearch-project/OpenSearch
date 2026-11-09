/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.HitsMetadata;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class SearchHitsProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithBasicFields() throws IOException {
        // Create SearchHits with basic fields
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(1, "test_id_1", null, null);
        hits[0].score(2.0f);
        ShardId shardId = new ShardId("test_index_1", "_na_", 1);
        SearchShardTarget searchShardTarget = new SearchShardTarget("test_node", shardId, null, null);
        hits[0].shard(searchShardTarget);

        hits[1] = new SearchHit(2, "test_id_2", null, null);
        hits[1].score(3.0f);
        ShardId shardId2 = new ShardId("test_index_2", "_na_", 1);
        SearchShardTarget searchShardTarget2 = new SearchShardTarget("test_node", shardId2, null, null);
        hits[1].shard(searchShardTarget2);

        TotalHits totalHits = new TotalHits(10, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(hits, totalHits, 3.0f);

        // Call the method under test
        HitsMetadata hitsMetadata = SearchHitsProtoUtils.toProto(searchHits);

        // Verify the result
        assertNotNull("HitsMetadata should not be null", hitsMetadata);
        assertEquals("Total hits value should match", 10, hitsMetadata.getTotal().getTotalHits().getValue());
        assertEquals(
            "Total hits relation should be EQUAL_TO",
            org.opensearch.protobufs.TotalHitsRelation.TOTAL_HITS_RELATION_EQ,
            hitsMetadata.getTotal().getTotalHits().getRelation()
        );
        // Max score is HitsMetadataMaxScore object with getFloat() method
        assertEquals("Max score should match", 3.0f, hitsMetadata.getMaxScore().getFloat(), 0.0f);
        assertEquals("Hits count should match", 2, hitsMetadata.getHitsCount());
        assertEquals("First hit ID should match", "test_id_1", hitsMetadata.getHits(0).getXId());
        assertEquals("Second hit ID should match", "test_id_2", hitsMetadata.getHits(1).getXId());
    }

    public void testToProtoWithNullTotalHits() throws IOException {
        // Create SearchHits with null totalHits
        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(1, "test_id", null, null);
        hits[0].score(2.0f);
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        SearchShardTarget searchShardTarget = new SearchShardTarget("test_node", shardId, null, null);
        hits[0].shard(searchShardTarget);

        SearchHits searchHits = new SearchHits(hits, null, 2.0f);

        // Call the method under test
        HitsMetadata hitsMetadata = SearchHitsProtoUtils.toProto(searchHits);

        // Verify the result
        assertNotNull("HitsMetadata should not be null", hitsMetadata);
        assertFalse("Total hits should not have value", hitsMetadata.getTotal().hasTotalHits());
        assertEquals("Max score should match", 2.0f, hitsMetadata.getMaxScore().getFloat(), 0.0f);
        assertEquals("Hits count should match", 1, hitsMetadata.getHitsCount());
    }

    public void testToProtoWithGreaterThanRelation() throws IOException {
        // Create SearchHits with GREATER_THAN_OR_EQUAL_TO relation
        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(1, "test_id", null, null);
        hits[0].score(2.0f);
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        SearchShardTarget searchShardTarget = new SearchShardTarget("test_node", shardId, null, null);
        hits[0].shard(searchShardTarget);

        TotalHits totalHits = new TotalHits(10, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        SearchHits searchHits = new SearchHits(hits, totalHits, 2.0f);

        // Call the method under test
        HitsMetadata hitsMetadata = SearchHitsProtoUtils.toProto(searchHits);

        // Verify the result
        assertNotNull("HitsMetadata should not be null", hitsMetadata);
        assertEquals("Total hits value should match", 10, hitsMetadata.getTotal().getTotalHits().getValue());
        assertEquals(
            "Total hits relation should be GREATER_THAN_OR_EQUAL_TO",
            org.opensearch.protobufs.TotalHitsRelation.TOTAL_HITS_RELATION_GTE,
            hitsMetadata.getTotal().getTotalHits().getRelation()
        );
    }

    public void testToProtoWithNaNMaxScore() throws IOException {
        // Create SearchHits with NaN maxScore
        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(1, "test_id", null, null);
        hits[0].score(2.0f);
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        SearchShardTarget searchShardTarget = new SearchShardTarget("test_node", shardId, null, null);
        hits[0].shard(searchShardTarget);

        TotalHits totalHits = new TotalHits(10, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(hits, totalHits, Float.NaN);

        // Call the method under test
        HitsMetadata hitsMetadata = SearchHitsProtoUtils.toProto(searchHits);

        // Verify the result
        assertNotNull("HitsMetadata should not be null", hitsMetadata);
        assertTrue("Max score should be null", hitsMetadata.getMaxScore().hasNullValue());
    }

    public void testToProtoWithEmptyHits() throws IOException {
        // Create SearchHits with empty hits array
        SearchHit[] hits = new SearchHit[0];
        TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
        SearchHits searchHits = new SearchHits(hits, totalHits, 0.0f);

        // Call the method under test
        HitsMetadata hitsMetadata = SearchHitsProtoUtils.toProto(searchHits);

        // Verify the result
        assertNotNull("HitsMetadata should not be null", hitsMetadata);
        assertEquals("Total hits value should match", 0, hitsMetadata.getTotal().getTotalHits().getValue());
        assertEquals("Hits count should be 0", 0, hitsMetadata.getHitsCount());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response.search;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.Hit;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SearchHitProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithBasicFields() throws IOException {
        // Create a SearchHit with basic fields
        SearchHit searchHit = new SearchHit(1, "test_id", null, null);
        searchHit.score(2.0f);
        searchHit.shard(new SearchShardTarget("test_node", new ShardId("test_index", "_na_", 0), null, null));
        searchHit.version(3);
        searchHit.setSeqNo(4);
        searchHit.setPrimaryTerm(5);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Index should match", "test_index", hit.getIndex());
        assertEquals("ID should match", "test_id", hit.getId());
        assertEquals("Version should match", 3, hit.getVersion());
        assertEquals("SeqNo should match", 4, hit.getSeqNo());
        assertEquals("PrimaryTerm should match", 5, hit.getPrimaryTerm());
        assertEquals("Score should match", 2.0f, hit.getScore().getFloatValue(), 0.0f);
    }

    public void testToProtoWithNullScore() throws IOException {
        // Create a SearchHit with NaN score
        SearchHit searchHit = new SearchHit(1);
        searchHit.score(Float.NaN);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Score should be null", hit.getScore().hasNullValue());
    }

    public void testToProtoWithSource() throws IOException {
        // Create a SearchHit with source
        SearchHit searchHit = new SearchHit(1);
        byte[] sourceBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        searchHit.sourceRef(new BytesArray(sourceBytes));

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should not be empty", hit.getSource().size() > 0);
        assertArrayEquals("Source bytes should match", sourceBytes, hit.getSource().toByteArray());
    }

    public void testToProtoWithClusterAlias() throws IOException {
        // Create a SearchHit with cluster alias
        SearchHit searchHit = new SearchHit(1);
        searchHit.shard(new SearchShardTarget("test_node", new ShardId("test_index", "_na_", 0), "test_cluster", null));

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Index with cluster alias should match", "test_cluster:test_index", hit.getIndex());
    }

    public void testToProtoWithUnassignedSeqNo() throws IOException {
        // Create a SearchHit with unassigned seqNo
        SearchHit searchHit = new SearchHit(1);
        searchHit.setSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertFalse("SeqNo should not be set", hit.hasSeqNo());
        assertFalse("PrimaryTerm should not be set", hit.hasPrimaryTerm());
    }

    public void testToProtoWithNullFields() throws IOException {
        // Create a SearchHit with null fields
        SearchHit searchHit = new SearchHit(1);
        // Don't set any fields

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Index should not be set", "", hit.getIndex());
        assertEquals("ID should not be set", "", hit.getId());
        assertFalse("Version should not be set", hit.hasVersion());
        assertFalse("SeqNo should not be set", hit.hasSeqNo());
        assertFalse("PrimaryTerm should not be set", hit.hasPrimaryTerm());
        assertFalse("Source should not be set", hit.hasSource());
    }
}

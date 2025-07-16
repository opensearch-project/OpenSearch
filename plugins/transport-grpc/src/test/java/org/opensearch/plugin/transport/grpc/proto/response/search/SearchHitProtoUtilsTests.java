/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response.search;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.Hit;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;

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

    public void testToProtoWithDocumentFields() throws IOException {
        // Create a SearchHit with document fields
        SearchHit searchHit = new SearchHit(1);

        // Add document fields
        List<Object> fieldValues = new ArrayList<>();
        fieldValues.add("value1");
        fieldValues.add("value2");
        searchHit.setDocumentField("field1", new DocumentField("field1", fieldValues));

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Fields should be set", hit.hasFields());
        assertTrue("Field1 should exist", hit.getFields().containsFields("field1"));
        assertEquals("Field1 should have 2 values", 2, hit.getFields().getFieldsOrThrow("field1").getListValue().getValueCount());
        assertEquals(
            "First value should match",
            "value1",
            hit.getFields().getFieldsOrThrow("field1").getListValue().getValue(0).getString()
        );
        assertEquals(
            "Second value should match",
            "value2",
            hit.getFields().getFieldsOrThrow("field1").getListValue().getValue(1).getString()
        );
    }

    public void testToProtoWithHighlightFields() throws IOException {
        // Create a SearchHit with highlight fields
        SearchHit searchHit = new SearchHit(1);

        // Add highlight fields
        Map<String, HighlightField> highlightFields = new HashMap<>();
        Text[] fragments = new Text[] { new Text("highlighted text") };
        highlightFields.put("field1", new HighlightField("field1", fragments));
        searchHit.highlightFields(highlightFields);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Should have 1 highlight field", 1, hit.getHighlightCount());
        assertTrue("Highlight field1 should exist", hit.containsHighlight("field1"));
        assertEquals("Highlight field1 should have 1 fragment", 1, hit.getHighlightOrThrow("field1").getStringArrayCount());
        assertEquals("Highlight fragment should match", "highlighted text", hit.getHighlightOrThrow("field1").getStringArray(0));
    }

    public void testToProtoWithMatchedQueries() throws IOException {
        // Create a SearchHit with matched queries
        SearchHit searchHit = new SearchHit(1);

        // Add matched queries
        searchHit.matchedQueries(new String[] { "query1", "query2" });

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Should have 2 matched queries", 2, hit.getMatchedQueriesCount());
        assertEquals("First matched query should match", "query1", hit.getMatchedQueries(0));
        assertEquals("Second matched query should match", "query2", hit.getMatchedQueries(1));
    }

    public void testToProtoWithExplanation() throws IOException {
        // Create a SearchHit with explanation
        SearchHit searchHit = new SearchHit(1);
        searchHit.shard(new SearchShardTarget("test_node", new ShardId("test_index", "_na_", 0), null, null));

        // Add explanation
        Explanation explanation = Explanation.match(1.0f, "explanation");
        searchHit.explanation(explanation);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Explanation should be set", hit.hasExplanation());
        assertEquals("Explanation value should match", 1.0, hit.getExplanation().getValue(), 0.0);
        assertEquals("Explanation description should match", "explanation", hit.getExplanation().getDescription());
    }

    public void testToProtoWithInnerHits() throws IOException {
        // Create a SearchHit with inner hits
        SearchHit searchHit = new SearchHit(1);

        // Add inner hits
        Map<String, SearchHits> innerHits = new HashMap<>();
        SearchHit[] innerHitsArray = new SearchHit[] { new SearchHit(2, "inner_id", null, null) };
        innerHits.put("inner_hit", new SearchHits(innerHitsArray, new TotalHits(1, EQUAL_TO), 1.0f));
        searchHit.setInnerHits(innerHits);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Should have 1 inner hit", 1, hit.getInnerHitsCount());
        assertTrue("Inner hit should exist", hit.containsInnerHits("inner_hit"));
        assertEquals("Inner hit should have 1 hit", 1, hit.getInnerHitsOrThrow("inner_hit").getHits().getHitsCount());
        assertEquals("Inner hit ID should match", "inner_id", hit.getInnerHitsOrThrow("inner_hit").getHits().getHits(0).getId());
    }

    public void testToProtoWithNestedIdentity() throws Exception {
        // Create a SearchHit with nested identity
        SearchHit.NestedIdentity nestedIdentity = new SearchHit.NestedIdentity("parent_field", 5, null);
        SearchHit searchHit = new SearchHit(1, "1", nestedIdentity, null, null);

        // Call the method under test
        Hit hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Nested identity should be set", hit.hasNested());
        assertEquals("Nested field should match", "parent_field", hit.getNested().getField());
        assertEquals("Nested offset should match", 5, hit.getNested().getOffset());
    }
}

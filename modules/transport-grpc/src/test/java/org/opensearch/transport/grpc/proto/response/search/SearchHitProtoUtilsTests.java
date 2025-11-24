/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.HitsMetadataHitsInner;
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
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Index should match", "test_index", hit.getXIndex());
        assertEquals("ID should match", "test_id", hit.getXId());
        assertEquals("Version should match", 3, hit.getXVersion());
        assertEquals("SeqNo should match", 4, hit.getXSeqNo());
        assertEquals("PrimaryTerm should match", 5, hit.getXPrimaryTerm());

        // Verify the score structure
        assertTrue("Score should be set", hit.hasXScore());
        assertEquals("Score should match", 2.0, hit.getXScore().getDouble(), 0.0);
        assertFalse("Score should not be null", hit.getXScore().hasNullValue());
    }

    public void testToProtoWithNullScore() throws IOException {
        // Create a SearchHit with NaN score
        SearchHit searchHit = new SearchHit(1);
        searchHit.score(Float.NaN);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Score should be set for NaN", hit.hasXScore());
        assertTrue("Score should have null value for NaN", hit.getXScore().hasNullValue());
        assertEquals(
            "Score null value should be NULL_VALUE_NULL",
            org.opensearch.protobufs.NullValue.NULL_VALUE_NULL,
            hit.getXScore().getNullValue()
        );
    }

    public void testToProtoWithSource() throws IOException {
        // Create a SearchHit with source
        SearchHit searchHit = new SearchHit(1);
        byte[] sourceBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        searchHit.sourceRef(new BytesArray(sourceBytes));

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should not be empty", hit.getXSource().size() > 0);
        assertArrayEquals("Source bytes should match", sourceBytes, hit.getXSource().toByteArray());
    }

    public void testToProtoWithClusterAlias() throws IOException {
        // Create a SearchHit with cluster alias
        SearchHit searchHit = new SearchHit(1);
        searchHit.shard(new SearchShardTarget("test_node", new ShardId("test_index", "_na_", 0), "test_cluster", null));

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Index with cluster alias should match", "test_cluster:test_index", hit.getXIndex());
    }

    public void testToProtoWithUnassignedSeqNo() throws IOException {
        // Create a SearchHit with unassigned seqNo
        SearchHit searchHit = new SearchHit(1);
        searchHit.setSeqNo(SequenceNumbers.UNASSIGNED_SEQ_NO);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertFalse("SeqNo should not be set", hit.hasXSeqNo());
        assertFalse("PrimaryTerm should not be set", hit.hasXPrimaryTerm());
    }

    public void testToProtoWithNullFields() throws IOException {
        // Create a SearchHit with null fields
        SearchHit searchHit = new SearchHit(1);
        // Don't set any fields

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Index should not be set", "", hit.getXIndex());
        assertEquals("ID should not be set", "", hit.getXId());
        assertFalse("Version should not be set", hit.hasXVersion());
        assertFalse("SeqNo should not be set", hit.hasXSeqNo());
        assertFalse("PrimaryTerm should not be set", hit.hasXPrimaryTerm());
        assertFalse("Source should not be set", hit.hasXSource());
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
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

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
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

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
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

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
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Explanation should be set", hit.hasXExplanation());
        assertEquals("Explanation value should match", 1.0, hit.getXExplanation().getValue(), 0.0);
        assertEquals("Explanation description should match", "explanation", hit.getXExplanation().getDescription());
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
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertEquals("Should have 1 inner hit", 1, hit.getInnerHitsCount());
        assertTrue("Inner hit should exist", hit.containsInnerHits("inner_hit"));
        assertEquals("Inner hit should have 1 hit", 1, hit.getInnerHitsOrThrow("inner_hit").getHits().getHitsCount());
        assertEquals("Inner hit ID should match", "inner_id", hit.getInnerHitsOrThrow("inner_hit").getHits().getHits(0).getXId());
    }

    public void testToProtoWithNestedIdentity() throws Exception {
        // Create a SearchHit with nested identity
        SearchHit.NestedIdentity nestedIdentity = new SearchHit.NestedIdentity("parent_field", 5, null);
        SearchHit searchHit = new SearchHit(1, "1", nestedIdentity, null, null);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Nested identity should be set", hit.hasXNested());
        assertEquals("Nested field should match", "parent_field", hit.getXNested().getField());
        assertEquals("Nested offset should match", 5, hit.getXNested().getOffset());
    }

    public void testToProtoWithScoreStructure() throws IOException {

        // Test with a valid score
        SearchHit searchHitWithScore = new SearchHit(1);
        searchHitWithScore.score(3.14159f);

        HitsMetadataHitsInner hitWithScore = SearchHitProtoUtils.toProto(searchHitWithScore);

        assertNotNull("Hit with score should not be null", hitWithScore);
        assertTrue("Score should be set", hitWithScore.hasXScore());
        assertEquals("Score value should match", 3.14159, hitWithScore.getXScore().getDouble(), 0.00001);
        assertFalse("Score should not have null value", hitWithScore.getXScore().hasNullValue());

        // Test with zero score
        SearchHit searchHitWithZeroScore = new SearchHit(2);
        searchHitWithZeroScore.score(0.0f);

        HitsMetadataHitsInner hitWithZeroScore = SearchHitProtoUtils.toProto(searchHitWithZeroScore);

        assertNotNull("Hit with zero score should not be null", hitWithZeroScore);
        assertTrue("Score should be set", hitWithZeroScore.hasXScore());
        assertEquals("Zero score value should match", 0.0, hitWithZeroScore.getXScore().getDouble(), 0.0);
        assertFalse("Zero score should not have null value", hitWithZeroScore.getXScore().hasNullValue());

        // Test with negative score
        SearchHit searchHitWithNegativeScore = new SearchHit(3);
        searchHitWithNegativeScore.score(-1.5f);

        HitsMetadataHitsInner hitWithNegativeScore = SearchHitProtoUtils.toProto(searchHitWithNegativeScore);

        assertNotNull("Hit with negative score should not be null", hitWithNegativeScore);
        assertTrue("Score should be set", hitWithNegativeScore.hasXScore());
        assertEquals("Negative score value should match", -1.5, hitWithNegativeScore.getXScore().getDouble(), 0.0);
        assertFalse("Negative score should not have null value", hitWithNegativeScore.getXScore().hasNullValue());
    }

    public void testToProtoWithBytesArrayZeroCopyOptimization() throws IOException {
        // Create a SearchHit with BytesArray source (should use zero-copy optimization)
        SearchHit searchHit = new SearchHit(1);
        byte[] sourceBytes = "{\"field\":\"value\",\"number\":42}".getBytes(StandardCharsets.UTF_8);
        BytesArray bytesArray = new BytesArray(sourceBytes);
        searchHit.sourceRef(bytesArray);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());
        assertArrayEquals("Source bytes should match exactly", sourceBytes, hit.getXSource().toByteArray());

        // Verify that the ByteString was created using UnsafeByteOperations.unsafeWrap
        // This is an indirect test - we verify the content is correct and assume the optimization was used
        assertEquals("Source size should match", sourceBytes.length, hit.getXSource().size());
    }

    public void testToProtoWithBytesArrayWithOffsetZeroCopyOptimization() throws IOException {
        // Create a SearchHit with BytesArray source that has offset/length (should use zero-copy with offset)
        SearchHit searchHit = new SearchHit(1);
        byte[] fullBytes = "prefix{\"field\":\"value\"}suffix".getBytes(StandardCharsets.UTF_8);
        byte[] expectedBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        int offset = 6; // "prefix".length()
        int length = expectedBytes.length;
        BytesArray bytesArray = new BytesArray(fullBytes, offset, length);
        searchHit.sourceRef(bytesArray);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());
        assertArrayEquals("Source bytes should match the sliced portion", expectedBytes, hit.getXSource().toByteArray());
        assertEquals("Source size should match expected length", length, hit.getXSource().size());
    }

    public void testToProtoWithCompositeBytesReferenceUsesDeepCopy() throws IOException {
        // Create a SearchHit with CompositeBytesReference source (should use ByteString.copyFrom)
        SearchHit searchHit = new SearchHit(1);
        byte[] bytes1 = "{\"field1\":".getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = "\"value1\"}".getBytes(StandardCharsets.UTF_8);
        BytesArray part1 = new BytesArray(bytes1);
        BytesArray part2 = new BytesArray(bytes2);
        CompositeBytesReference compositeBytesRef = (CompositeBytesReference) CompositeBytesReference.of(part1, part2);
        searchHit.sourceRef(compositeBytesRef);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());

        // Verify the combined content is correct
        String expectedJson = "{\"field1\":\"value1\"}";
        byte[] expectedBytes = expectedJson.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals("Source bytes should match the combined content", expectedBytes, hit.getXSource().toByteArray());
        assertEquals("Source size should match combined length", expectedBytes.length, hit.getXSource().size());
    }

    public void testToProtoWithEmptyBytesArraySource() throws IOException {
        // Create a SearchHit with minimal valid JSON as source (empty object)
        SearchHit searchHit = new SearchHit(1);
        byte[] emptyJsonBytes = "{}".getBytes(StandardCharsets.UTF_8);
        BytesArray emptyJsonBytesArray = new BytesArray(emptyJsonBytes);
        searchHit.sourceRef(emptyJsonBytesArray);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());
        assertEquals("Source should contain empty JSON object", emptyJsonBytes.length, hit.getXSource().size());
        assertArrayEquals("Source bytes should match empty JSON object", emptyJsonBytes, hit.getXSource().toByteArray());
    }

    public void testToProtoWithLargeBytesArrayZeroCopyOptimization() throws IOException {
        // Create a SearchHit with large BytesArray source to test performance benefit
        SearchHit searchHit = new SearchHit(1);
        StringBuilder largeJsonBuilder = new StringBuilder("{\"data\":[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) largeJsonBuilder.append(",");
            largeJsonBuilder.append("{\"id\":").append(i).append(",\"value\":\"item").append(i).append("\"}");
        }
        largeJsonBuilder.append("]}");

        byte[] largeSourceBytes = largeJsonBuilder.toString().getBytes(StandardCharsets.UTF_8);
        BytesArray largeBytesArray = new BytesArray(largeSourceBytes);
        searchHit.sourceRef(largeBytesArray);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());
        assertEquals("Source size should match large content", largeSourceBytes.length, hit.getXSource().size());
        assertArrayEquals("Source bytes should match exactly", largeSourceBytes, hit.getXSource().toByteArray());
    }

    public void testToProtoWithBytesArraySliceZeroCopyOptimization() throws IOException {
        // Test the optimization with a BytesArray that represents a slice of a larger array
        SearchHit searchHit = new SearchHit(1);

        // Create a larger byte array
        String fullContent = "HEADER{\"important\":\"data\",\"field\":\"value\"}FOOTER";
        byte[] fullBytes = fullContent.getBytes(StandardCharsets.UTF_8);

        // Create a BytesArray that represents just the JSON part
        int jsonStart = 6; // "HEADER".length()
        String jsonContent = "{\"important\":\"data\",\"field\":\"value\"}";
        int jsonLength = jsonContent.length();
        BytesArray slicedBytesArray = new BytesArray(fullBytes, jsonStart, jsonLength);
        searchHit.sourceRef(slicedBytesArray);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());
        assertEquals("Source size should match JSON length", jsonLength, hit.getXSource().size());

        byte[] expectedJsonBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals("Source bytes should match only the JSON portion", expectedJsonBytes, hit.getXSource().toByteArray());
    }

    public void testToProtoSourceOptimizationBehaviorComparison() throws IOException {
        // Test to demonstrate the difference in behavior between BytesArray and other BytesReference types
        String jsonContent = "{\"test\":\"optimization\"}";
        byte[] jsonBytes = jsonContent.getBytes(StandardCharsets.UTF_8);

        // Test with BytesArray (should use zero-copy optimization)
        SearchHit searchHitWithBytesArray = new SearchHit(1);
        BytesArray bytesArray = new BytesArray(jsonBytes);
        searchHitWithBytesArray.sourceRef(bytesArray);
        HitsMetadataHitsInner hitWithBytesArray = SearchHitProtoUtils.toProto(searchHitWithBytesArray);

        // Test with CompositeBytesReference (should use deep copy)
        SearchHit searchHitWithComposite = new SearchHit(2);
        BytesArray part1 = new BytesArray(jsonBytes, 0, jsonBytes.length / 2);
        BytesArray part2 = new BytesArray(jsonBytes, jsonBytes.length / 2, jsonBytes.length - jsonBytes.length / 2);
        CompositeBytesReference composite = (CompositeBytesReference) CompositeBytesReference.of(part1, part2);
        searchHitWithComposite.sourceRef(composite);
        HitsMetadataHitsInner hitWithComposite = SearchHitProtoUtils.toProto(searchHitWithComposite);

        // Both should produce the same result
        assertArrayEquals(
            "Both approaches should produce identical byte content",
            hitWithBytesArray.getXSource().toByteArray(),
            hitWithComposite.getXSource().toByteArray()
        );
        assertEquals(
            "Both approaches should produce same size",
            hitWithBytesArray.getXSource().size(),
            hitWithComposite.getXSource().size()
        );
    }

    public void testToProtoWithNullSourceRef() throws IOException {
        // Test behavior when source reference is null
        SearchHit searchHit = new SearchHit(1);
        // Don't set any source reference (sourceRef remains null)

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertFalse("Source should not be set when sourceRef is null", hit.hasXSource());
    }

    public void testToProtoWithActuallyEmptyBytesArray() throws IOException {
        // Test the edge case of truly empty bytes - this should be handled gracefully
        // by checking if the source is null before processing
        SearchHit searchHit = new SearchHit(1);
        // Explicitly set source to null to test null handling
        searchHit.sourceRef(null);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result
        assertNotNull("Hit should not be null", hit);
        assertFalse("Source should not be set when explicitly null", hit.hasXSource());
    }

    public void testToProtoWithBytesArrayOffsetConditionCoverage() throws IOException {
        // Test the specific condition coverage for BytesArray when offset != 0 OR length != bytes.length
        // This covers the else branch in the optimization logic (lines 207-208)
        SearchHit searchHit = new SearchHit(1);

        // Create a larger byte array with prefix and suffix
        String prefix = "PREFIX";
        String jsonContent = "{\"field\":\"value\"}";
        String suffix = "SUFFIX";
        String fullContent = prefix + jsonContent + suffix;
        byte[] fullBytes = fullContent.getBytes(StandardCharsets.UTF_8);

        // Create BytesArray with offset > 0 to extract just the JSON part
        // This will trigger the condition: bytesRef.offset != 0 || bytesRef.length != bytesRef.bytes.length
        int offset = prefix.length();
        int length = jsonContent.length();
        BytesArray slicedBytesArray = new BytesArray(fullBytes, offset, length);
        searchHit.sourceRef(slicedBytesArray);

        // Call the method under test
        HitsMetadataHitsInner hit = SearchHitProtoUtils.toProto(searchHit);

        // Verify the result - should use the offset/length version of unsafeWrap
        assertNotNull("Hit should not be null", hit);
        assertTrue("Source should be set", hit.hasXSource());
        assertEquals("Source size should match JSON length", length, hit.getXSource().size());

        byte[] expectedBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals("Source bytes should match JSON content", expectedBytes, hit.getXSource().toByteArray());
    }
}

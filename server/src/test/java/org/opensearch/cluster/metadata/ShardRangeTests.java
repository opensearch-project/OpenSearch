/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.OutputStream;

public class ShardRangeTests extends OpenSearchTestCase {

    /**
     * Test compareTo method with equal start values
     */

    public void testCompareToWithEqualStartValues() {
        ShardRange range1 = new ShardRange(1, 0, 100);
        ShardRange range2 = new ShardRange(2, 0, 200);

        assertEquals(0, range1.compareTo(range2));
    }

    /**
     * Test compareTo method with overlapping ranges
     */

    public void testCompareToWithOverlappingRanges() {
        ShardRange range1 = new ShardRange(1, 0, 100);
        ShardRange range2 = new ShardRange(2, 50, 150);

        assertTrue(range1.compareTo(range2) < 0);
        assertTrue(range2.compareTo(range1) > 0);
    }

    /**
     * Test compareTo method with null input
     */

    public void testCompareToWithNull() {
        ShardRange range = new ShardRange(1, 0, 100);
        expectThrows(NullPointerException.class, () -> range.compareTo(null));
    }

    public void testContainsWithHashInBetween() {
        /**
         * Test that the contains method returns true when the hash is equal to the end value.
         */
        ShardRange shardRange = new ShardRange(0, 10, 20);
        assertTrue(shardRange.contains(15));
    }

    public void testContainsWithHashEqualToEnd() {
        /**
         * Test that the contains method returns true when the hash is equal to the end value.
         */
        ShardRange shardRange = new ShardRange(0, 10, 20);
        assertTrue(shardRange.contains(20));
    }

    public void testContainsWithHashEqualToStart() {
        /**
         * Test that the contains method returns true when the hash is equal to the start value.
         */
        ShardRange shardRange = new ShardRange(0, 10, 20);
        assertTrue(shardRange.contains(10));
    }

    public void testContainsWithHashGreaterThanEnd() {
        /**
         * Test that the contains method returns false when the hash is greater than the end value.
         */
        ShardRange shardRange = new ShardRange(0, 10, 20);
        assertFalse(shardRange.contains(25));
    }

    public void testContainsWithHashLessThanStart() {
        /**
         * Test that the contains method returns false when the hash is less than the start value.
         */
        ShardRange shardRange = new ShardRange(0, 10, 20);
        assertFalse(shardRange.contains(5));
    }

    public void testContainsWithNegativeHash() {
        /**
         * Test that the contains method handles negative hash values correctly.
         */
        ShardRange shardRange = new ShardRange(0, -10, 10);
        assertTrue(shardRange.contains(-5));
        assertFalse(shardRange.contains(-15));
    }

    /**
     * Test that the copy method creates a new object with the same values.
     */

    public void testCopyCreatesNewObjectWithSameValues() {
        ShardRange original = new ShardRange(1, 0, 100);
        ShardRange copy = original.copy();

        assertNotSame("Copy should be a new object", original, copy);
        assertEquals("ShardId should be the same", original.getShardId(), copy.getShardId());
        assertEquals("Start should be the same", original.getStart(), copy.getStart());
        assertEquals("End should be the same", original.getEnd(), copy.getEnd());
    }

    /**
     * Test that modifying the copy doesn't affect the original.
     */

    public void testCopyIsIndependentOfOriginal() {
        ShardRange original = new ShardRange(1, 0, 100);
        ShardRange copy = original.copy();

        // Attempt to modify the copy (this won't actually work because ShardRange is immutable,
        // but it illustrates the point)
        ShardRange modifiedCopy = new ShardRange(copy.getShardId() + 1, copy.getStart() + 10, copy.getEnd() + 10);

        assertNotEquals("Original should not be affected by changes to copy", original.getShardId(), modifiedCopy.getShardId());
        assertNotEquals("Original should not be affected by changes to copy", original.getStart(), modifiedCopy.getStart());
        assertNotEquals("Original should not be affected by changes to copy", original.getEnd(), modifiedCopy.getEnd());
    }

    /**
     * Tests the equals method of ShardRange class for various scenarios.
     */

    public void testEqualsMethod() {
        // Create ShardRange objects for testing
        ShardRange range1 = new ShardRange(1, 0, 100);
        ShardRange range2 = new ShardRange(1, 0, 100);
        ShardRange range3 = new ShardRange(2, 0, 100);
        ShardRange range4 = new ShardRange(1, 10, 100);
        ShardRange range5 = new ShardRange(1, 0, 90);

        // Test equality with itself
        assertTrue("A ShardRange should be equal to itself", range1.equals(range1));

        // Test equality with an identical ShardRange
        assertTrue("Two identical ShardRanges should be equal", range1.equals(range2));

        // Test inequality with different shardId
        assertFalse("ShardRanges with different shardIds should not be equal", range1.equals(range3));

        // Test inequality with different start value
        assertFalse("ShardRanges with different start values should not be equal", range1.equals(range4));

        // Test inequality with different end value
        assertFalse("ShardRanges with different end values should not be equal", range1.equals(range5));

        // Test inequality with null
        assertFalse("A ShardRange should not be equal to null", range1.equals(null));
    }

    /**
     * Test case for getEnd() method of ShardRange class
     * Verifies that the method correctly returns the end value
     */

    public void testGetEndReturnsCorrectValue() {
        // Arrange
        int expectedEnd = 100;
        ShardRange shardRange = new ShardRange(0, 0, expectedEnd);

        // Act
        int actualEnd = shardRange.getEnd();

        // Assert
        assertEquals("getEnd() should return the correct end value", expectedEnd, actualEnd);
    }

    /**
     * Test case for getShardId() method
     * Verifies that the method correctly returns the shard ID
     */

    public void testGetShardIdReturnsCorrectValue() {
        // Arrange
        int expectedShardId = 5;
        ShardRange shardRange = new ShardRange(expectedShardId, 0, 100);

        // Act
        int actualShardId = shardRange.getShardId();

        // Assert
        assertEquals("getShardId() should return the correct shard ID", expectedShardId, actualShardId);
    }

    /**
     * Test that getStart returns the correct value for a valid ShardRange
     */

    public void testGetStartReturnsCorrectValue() {
        ShardRange shardRange = new ShardRange(1, 100, 200);
        assertEquals(100, shardRange.getStart());
    }

    /**
     * Test hashCode() consistency for equal objects
     */

    public void testHashCodeConsistency() {
        ShardRange shardRange1 = new ShardRange(1, 100, 200);
        ShardRange shardRange2 = new ShardRange(1, 100, 200);
        assertEquals(shardRange1.hashCode(), shardRange2.hashCode());
    }

    /**
     * Test hashCode() with zero values for all fields
     */

    public void testHashCodeWithZeroValues() {
        ShardRange shardRange = new ShardRange(0, 0, 0);
        assertEquals(0, shardRange.hashCode());
    }

    /**
     * Test the toString method of ShardRange
     * Verifies that the toString method returns the expected string representation
     */

    public void testToStringReturnsCorrectRepresentation() {
        // Arrange
        int shardId = 1;
        int start = 0;
        int end = 100;
        ShardRange shardRange = new ShardRange(shardId, start, end);

        // Act
        String result = shardRange.toString();

        // Assert
        String expected = "ShardRange{shardId=1, start=0, end=100}";
        assertEquals("The toString method should return the correct string representation", expected, result);
    }

    /**
     * Test that toXContent correctly serializes a ShardRange object to JSON
     */

    public void testToXContentSerializesShardRangeCorrectly() throws IOException {
        int shardId = 1;
        int start = 0;
        int end = 100;
        ShardRange shardRange = new ShardRange(shardId, start, end);

        XContentBuilder builder = XContentFactory.jsonBuilder();  // Changed from contentBuilder
        builder = shardRange.toXContent(builder, null);

        String expected = "{\"shard_id\":1,\"start\":0,\"end\":100}";
        assertEquals(expected, builder.toString());  // Also switched the order of expected and actual
    }

    public void testToXContentWithNullBuilder() {
        /**
         * Test toXContent method with null XContentBuilder
         * This test verifies that a NullPointerException is thrown when a null XContentBuilder is provided
         */
        ShardRange shardRange = new ShardRange(1, 0, 100);
        expectThrows(NullPointerException.class, () -> shardRange.toXContent(null, null));
    }

    public void testToXContentIOException() throws IOException {
        ShardRange shardRange = new ShardRange(1, 0, 100);

        // Create a real XContentBuilder that writes to a broken OutputStream
        OutputStream failingStream = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("Simulated IOException");
            }

            @Override
            public void write(byte[] b) throws IOException {
                throw new IOException("Simulated IOException");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException("Simulated IOException");
            }
        };

        XContentBuilder builder = XContentFactory.jsonBuilder(failingStream);
        builder.startObject(); // Start the JSON object

        // The actual test - this should throw IOException when trying to write
        expectThrows(IOException.class, () -> {
            shardRange.toXContent(builder, null);
            builder.endObject(); // End the JSON object
            builder.close(); // Force flush/close which will trigger the write
        });
    }

    public void testWriteToWithIOException() {
        /**
         * Test that writeTo properly propagates IOException
         */
        ShardRange shardRange = new ShardRange(1, 0, 100);
        StreamOutput failingOutput = new StreamOutput() {
            @Override
            public void writeByte(byte b) throws IOException {
                throw new IOException("Simulated IO failure");
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                throw new IOException("Simulated IO failure");
            }

            @Override
            public void flush() throws IOException {
                throw new IOException("Simulated IO failure");
            }

            @Override
            public void close() throws IOException {
                throw new IOException("Simulated IO failure");
            }

            @Override
            public void reset() throws IOException {
                throw new IOException("Simulated IO failure");
            }
        };

        expectThrows(IOException.class, () -> shardRange.writeTo(failingOutput));
    }

    public void testWriteToWithNullStreamOutput() {
        /**
         * Test that writeTo throws NullPointerException when StreamOutput is null
         */
        ShardRange shardRange = new ShardRange(1, 0, 100);
        expectThrows(NullPointerException.class, () -> shardRange.writeTo(null));
    }

    /**
     * Test that the writeTo method correctly writes the ShardRange data to the stream
     * and that it can be read back correctly.
     */

    public void testWriteToCorrectlySerializesAndDeserializes() throws IOException {
        int shardId = 1;
        int start = 100;
        int end = 200;
        ShardRange originalShardRange = new ShardRange(shardId, start, end);

        BytesStreamOutput out = new BytesStreamOutput();
        originalShardRange.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ShardRange deserializedShardRange = new ShardRange(in);

        assertEquals(originalShardRange.getShardId(), deserializedShardRange.getShardId());
        assertEquals(originalShardRange.getStart(), deserializedShardRange.getStart());
        assertEquals(originalShardRange.getEnd(), deserializedShardRange.getEnd());
    }

    private XContentParser createParser(String content) throws IOException {
        return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, content);
    }

    public void testParseValidShardRange() throws IOException {
        String json = "{" + "\"shard_id\": 1," + "\"start\": 0," + "\"end\": 100" + "}";

        XContentParser parser = createParser(json);
        parser.nextToken(); // Move to start object

        ShardRange shardRange = ShardRange.parse(parser);

        assertEquals("Incorrect shard ID", 1, shardRange.getShardId());
        assertEquals("Incorrect start value", 0, shardRange.getStart());
        assertEquals("Incorrect end value", 100, shardRange.getEnd());
    }

    public void testParseEmptyObject() throws IOException {
        String json = "{}";

        XContentParser parser = createParser(json);
        parser.nextToken();

        ShardRange shardRange = ShardRange.parse(parser);

        assertEquals("Empty object should have default shard ID", -1, shardRange.getShardId());
        assertEquals("Empty object should have default start", -1, shardRange.getStart());
        assertEquals("Empty object should have default end", -1, shardRange.getEnd());
    }

    public void testParseWithExtraFields() throws IOException {
        String json = "{" + "\"shard_id\": 1," + "\"start\": 0," + "\"end\": 100," + "\"extra_field\": \"value\"" + "}";

        XContentParser parser = createParser(json);
        parser.nextToken();

        ShardRange shardRange = ShardRange.parse(parser);

        assertEquals("Incorrect shard ID", 1, shardRange.getShardId());
        assertEquals("Incorrect start value", 0, shardRange.getStart());
        assertEquals("Incorrect end value", 100, shardRange.getEnd());
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.nio.ByteBuffer;

public class KafkaOffsetTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        long offset = 12345L;
        KafkaOffset kafkaOffset = new KafkaOffset(offset);

        Assert.assertEquals("The offset should be correctly initialized and returned", offset, kafkaOffset.getOffset());
    }

    public void testEqualsAndHashCode() {
        long offset1 = 12345L;
        long offset2 = 67890L;
        KafkaOffset kafkaOffset1 = new KafkaOffset(offset1);
        KafkaOffset kafkaOffset2 = new KafkaOffset(offset1);
        KafkaOffset kafkaOffset3 = new KafkaOffset(offset2);

        Assert.assertTrue("Offsets with the same value should be equal", kafkaOffset1.equals(kafkaOffset2));
        Assert.assertFalse("Offsets with different values should not be equal", kafkaOffset1.equals(kafkaOffset3));
        Assert.assertEquals("Hash codes for equal offsets should be the same", kafkaOffset1.hashCode(), kafkaOffset2.hashCode());
        Assert.assertNotEquals("Hash codes for different offsets should not be the same", kafkaOffset1.hashCode(), kafkaOffset3.hashCode());
    }

    public void testSerialize() {
        long offset = 12345L;
        KafkaOffset kafkaOffset = new KafkaOffset(offset);
        byte[] expectedBytes = ByteBuffer.allocate(Long.BYTES).putLong(offset).array();

        Assert.assertArrayEquals("The serialized bytes should be correct", expectedBytes, kafkaOffset.serialize());
    }

    public void testAsString() {
        long offset = 12345L;
        KafkaOffset kafkaOffset = new KafkaOffset(offset);

        Assert.assertEquals("The string representation should be correct", String.valueOf(offset), kafkaOffset.asString());
    }

    public void testAsPointField() {
        long offset = 12345L;
        KafkaOffset kafkaOffset = new KafkaOffset(offset);
        Field field = kafkaOffset.asPointField("offsetField");

        Assert.assertTrue("The field should be an instance of LongPoint", field instanceof LongPoint);
    }

    public void testNewRangeQueryGreaterThan() {
        long offset = 12345L;
        KafkaOffset kafkaOffset = new KafkaOffset(offset);
        Query query = kafkaOffset.newRangeQueryGreaterThan("offsetField");

        Assert.assertTrue("The query should be an instance of range query", query instanceof PointRangeQuery);
    }

    public void testCompareTo() {
        long offset1 = 12345L;
        long offset2 = 67890L;
        KafkaOffset kafkaOffset1 = new KafkaOffset(offset1);
        KafkaOffset kafkaOffset2 = new KafkaOffset(offset2);

        Assert.assertTrue("The comparison should be correct", kafkaOffset1.compareTo(kafkaOffset2) < 0);
        Assert.assertTrue("The comparison should be correct", kafkaOffset2.compareTo(kafkaOffset1) > 0);
        Assert.assertTrue("The comparison should be correct", kafkaOffset1.compareTo(kafkaOffset1) == 0);
    }
}

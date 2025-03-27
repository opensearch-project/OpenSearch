/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;

public class SequenceNumberTests extends OpenSearchTestCase {
    public void testConstructorAndGetters() {
        String sequenceNumber = "12345";
        SequenceNumber seqNum = new SequenceNumber(sequenceNumber);

        Assert.assertEquals("The sequence number should be correctly initialized and returned", sequenceNumber, seqNum.getSequenceNumber());
    }

    public void testEqualsAndHashCode() {
        String sequenceNumber1 = "12345";
        String sequenceNumber2 = "67890";
        SequenceNumber seqNum1 = new SequenceNumber(sequenceNumber1);
        SequenceNumber seqNum2 = new SequenceNumber(sequenceNumber1);
        SequenceNumber seqNum3 = new SequenceNumber(sequenceNumber2);

        Assert.assertTrue("Sequence numbers with the same value should be equal", seqNum1.equals(seqNum2));
        Assert.assertFalse("Sequence numbers with different values should not be equal", seqNum1.equals(seqNum3));
        Assert.assertEquals("Hash codes for equal sequence numbers should be the same", seqNum1.hashCode(), seqNum2.hashCode());
        Assert.assertNotEquals("Hash codes for different sequence numbers should not be the same", seqNum1.hashCode(), seqNum3.hashCode());
    }

    public void testSerialize() {
        String sequenceNumber = "12345";
        SequenceNumber seqNum = new SequenceNumber(sequenceNumber);
        byte[] expectedBytes = sequenceNumber.getBytes(StandardCharsets.UTF_8);

        Assert.assertArrayEquals("The serialized bytes should be correct", expectedBytes, seqNum.serialize());
    }

    public void testAsString() {
        String sequenceNumber = "12345";
        SequenceNumber seqNum = new SequenceNumber(sequenceNumber);

        Assert.assertEquals("The string representation should be correct", sequenceNumber, seqNum.asString());
    }

    public void testCompareTo() {
        String sequenceNumber1 = "12345";
        String sequenceNumber2 = "67890";
        SequenceNumber seqNum1 = new SequenceNumber(sequenceNumber1);
        SequenceNumber seqNum2 = new SequenceNumber(sequenceNumber2);

        Assert.assertTrue("The comparison should be correct", seqNum1.compareTo(seqNum2) < 0);
        Assert.assertTrue("The comparison should be correct", seqNum2.compareTo(seqNum1) > 0);
        Assert.assertTrue("The comparison should be correct", seqNum1.compareTo(seqNum1) == 0);
    }

    public void testAsPointField() {
        String sequenceNumber = "12345";
        SequenceNumber seqNum = new SequenceNumber(sequenceNumber);
        Field field = seqNum.asPointField("sequenceNumberField");

        Assert.assertTrue("The field should be an instance of KeywordField", field instanceof KeywordField);
        Assert.assertEquals("The field name should be correct", "sequenceNumberField", field.name());
        Assert.assertEquals("The field value should be correct", sequenceNumber, field.stringValue());
    }

    public void testNewRangeQueryGreaterThan() {
        String sequenceNumber = "12345";
        SequenceNumber seqNum = new SequenceNumber(sequenceNumber);
        Query query = seqNum.newRangeQueryGreaterThan("sequenceNumberField");

        Assert.assertTrue("The query should be an instance of TermRangeQuery", query instanceof TermRangeQuery);
        TermRangeQuery termRangeQuery = (TermRangeQuery) query;
        Assert.assertEquals("The field name should be correct", "sequenceNumberField", termRangeQuery.getField());
        Assert.assertEquals("The lower term should be correct", sequenceNumber, termRangeQuery.getLowerTerm().utf8ToString());
        Assert.assertNull("The upper term should be null", termRangeQuery.getUpperTerm());
        Assert.assertFalse("The lower term should be exclusive", termRangeQuery.includesLower());
        Assert.assertTrue("The upper term should be inclusive", termRangeQuery.includesUpper());
    }
}

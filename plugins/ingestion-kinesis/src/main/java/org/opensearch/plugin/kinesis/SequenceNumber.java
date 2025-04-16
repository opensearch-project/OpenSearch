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
import org.opensearch.index.IngestionShardPointer;

import java.nio.charset.StandardCharsets;

/**
 * Kinesis sequence number.
 */
public class SequenceNumber implements IngestionShardPointer {

    private final String sequenceNumber;
    /** constant denoting non-existing sequence number */
    public static final SequenceNumber NON_EXISTING_SEQUENCE_NUMBER = new SequenceNumber("non-existing-sequence-number");

    /**
     * Constructor
     *
     * @param sequenceNumber the sequence number
     */
    public SequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * Get the sequence number
     *
     * @return the sequence number
     */
    public String getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public byte[] serialize() {
        return sequenceNumber.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String asString() {
        return sequenceNumber;
    }

    @Override
    public Field asPointField(String fieldName) {
        return new KeywordField(fieldName, sequenceNumber, KeywordField.Store.YES);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return TermRangeQuery.newStringRange(fieldName, sequenceNumber, null, false, true);
    }

    @Override
    public String toString() {
        return "SequenceNumber{" + "sequenceNumber=" + sequenceNumber + '}';
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        if (o == null) {
            throw new IllegalArgumentException("the pointer is null");
        }
        if (!(o instanceof SequenceNumber)) {
            throw new IllegalArgumentException("the pointer is of type " + o.getClass() + " and not SequenceNumber");
        }
        SequenceNumber other = (SequenceNumber) o;
        return sequenceNumber.compareTo(other.sequenceNumber);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SequenceNumber that = (SequenceNumber) o;
        return sequenceNumber == that.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return sequenceNumber.hashCode();
    }
}

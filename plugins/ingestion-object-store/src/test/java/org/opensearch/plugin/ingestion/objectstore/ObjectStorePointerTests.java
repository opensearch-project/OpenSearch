/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link ObjectStorePointer} covering serialization round-trips, malformed encodings, ordering,
 * and value semantics.
 */
public class ObjectStorePointerTests extends OpenSearchTestCase {

    public void testAccessors() {
        ObjectStorePointer pointer = new ObjectStorePointer("key", 42);
        assertThat(pointer.getObjectKey(), equalTo("key"));
        assertThat(pointer.getByteOffset(), is(42L));
    }

    public void testNullKeyThrows() {
        expectThrows(NullPointerException.class, () -> new ObjectStorePointer(null, 0));
    }

    public void testNegativeOffsetThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStorePointer("k", -1));
        assertThat(e.getMessage(), containsString("byteOffset must not be negative"));
    }

    public void testSerializeDeserializeRoundTrip() {
        ObjectStorePointer original = new ObjectStorePointer("path/to/object.ndjson", 12345);
        assertThat(ObjectStorePointer.deserialize(original.serialize()), equalTo(original));
    }

    public void testFromStringMalformedInputGivesClearError() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ObjectStorePointer.fromString("@@@not-base64@@@"));
        assertThat(e.getMessage(), containsString("malformed object_store pointer"));
    }

    public void testRoundTripWithNullByteInKey() {
        ObjectStorePointer original = new ObjectStorePointer("a\0b", 7);
        assertThat(ObjectStorePointer.deserialize(original.serialize()), equalTo(original));
    }

    public void testRoundTripEmptyKey() {
        ObjectStorePointer original = new ObjectStorePointer("", 0);
        assertThat(ObjectStorePointer.deserialize(original.serialize()), equalTo(original));
    }

    public void testRoundTripWithMultiByteUtf8Key() {
        ObjectStorePointer original = new ObjectStorePointer("prefix/é中😀.ndjson", 123456789L);
        assertThat(ObjectStorePointer.deserialize(original.serialize()), equalTo(original));
        assertThat(ObjectStorePointer.fromString(original.asString()), equalTo(original));
    }

    public void testEncodingIsOrderPreservingForOffsetsAndKeys() {
        ObjectStorePointer a0 = new ObjectStorePointer("a", 0);
        ObjectStorePointer a1 = new ObjectStorePointer("a", 1);
        ObjectStorePointer aa0 = new ObjectStorePointer("aa", 0);
        assertThat(Arrays.compareUnsigned(a0.serialize(), a1.serialize()), is(lessThan(0)));
        assertThat(Arrays.compareUnsigned(a1.serialize(), aa0.serialize()), is(lessThan(0)));
        assertThat(a0.compareTo(a1), is(lessThan(0)));
        assertThat(a1.compareTo(aa0), is(lessThan(0)));
    }

    public void testTerminatorSortsBeforeAnyRealKeyByte() {
        ObjectStorePointer shorter = new ObjectStorePointer("a", 0);
        ObjectStorePointer longer = new ObjectStorePointer("ab", 0);
        assertThat(Arrays.compareUnsigned(shorter.serialize(), longer.serialize()), is(lessThan(0)));
    }

    public void testAsStringFromStringRoundTrip() {
        ObjectStorePointer original = new ObjectStorePointer("key", 99);
        assertThat(ObjectStorePointer.fromString(original.asString()), equalTo(original));
    }

    public void testDeserializeNullThrows() {
        expectThrows(NullPointerException.class, () -> ObjectStorePointer.deserialize(null));
    }

    public void testFromStringNullThrows() {
        expectThrows(NullPointerException.class, () -> ObjectStorePointer.fromString(null));
    }

    public void testDeserializeDanglingEscapeThrows() {
        byte[] bytes = new byte[] { 'a', 0x00 };
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ObjectStorePointer.deserialize(bytes));
        assertThat(e.getMessage(), containsString("dangling escape byte"));
    }

    public void testDeserializeInvalidEscapeSequenceThrows() {
        byte[] bytes = new byte[] { 0x00, 0x05, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0 };
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ObjectStorePointer.deserialize(bytes));
        assertThat(e.getMessage(), containsString("invalid escape sequence"));
    }

    public void testDeserializeMissingTerminatorThrows() {
        byte[] bytes = "key".getBytes(StandardCharsets.UTF_8);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ObjectStorePointer.deserialize(bytes));
        assertThat(e.getMessage(), containsString("missing key terminator"));
    }

    public void testDeserializeWrongOffsetLengthThrows() {
        byte[] bytes = new byte[] { 0x00, 0x00, 0, 0, 0 };
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ObjectStorePointer.deserialize(bytes));
        assertThat(e.getMessage(), containsString("offset bytes"));
    }

    public void testCompareToNullThrowsNullPointerExceptionPerComparableContract() {
        ObjectStorePointer pointer = new ObjectStorePointer("k", 0);
        expectThrows(NullPointerException.class, () -> pointer.compareTo(null));
    }

    public void testCompareToWrongTypeThrowsClassCastExceptionPerComparableContract() {
        ObjectStorePointer pointer = new ObjectStorePointer("k", 0);
        IngestionShardPointer other = new IngestionShardPointer() {
            @Override
            public byte[] serialize() {
                return new byte[0];
            }

            @Override
            public String asString() {
                return "";
            }

            @Override
            public Field asPointField(String fieldName) {
                return null;
            }

            @Override
            public Query newRangeQueryGreaterThan(String fieldName) {
                return null;
            }

            @Override
            public int compareTo(IngestionShardPointer o) {
                return 0;
            }
        };
        ClassCastException e = expectThrows(ClassCastException.class, () -> pointer.compareTo(other));
        assertThat(e.getMessage(), containsString("not ObjectStorePointer"));
    }

    public void testCompareToOrdersByKeyThenOffset() {
        ObjectStorePointer a = new ObjectStorePointer("a", 100);
        ObjectStorePointer b = new ObjectStorePointer("b", 0);
        assertThat(a.compareTo(b), is(lessThan(0)));
        assertThat(b.compareTo(a), is(greaterThan(0)));

        ObjectStorePointer low = new ObjectStorePointer("k", 1);
        ObjectStorePointer high = new ObjectStorePointer("k", 2);
        assertThat(low.compareTo(high), is(lessThan(0)));
        assertThat(high.compareTo(low), is(greaterThan(0)));
        assertThat(low.compareTo(new ObjectStorePointer("k", 1)), is(0));
    }

    public void testPointFieldCarriesTheEncodingUnderTheGivenName() {
        ObjectStorePointer pointer = new ObjectStorePointer("k", 5);
        Field field = pointer.asPointField("offset_field");
        assertThat(field.name(), is("offset_field"));
        assertThat(field.binaryValue(), equalTo(new BytesRef(pointer.serialize())));
        assertThat(field.fieldType().stored(), is(true));
    }

    public void testRangeQueryIsExclusiveAboveThisPointerOnTheGivenField() {
        ObjectStorePointer pointer = new ObjectStorePointer("k", 5);
        TermRangeQuery query = (TermRangeQuery) pointer.newRangeQueryGreaterThan("offset_field");
        assertThat(query.getField(), is("offset_field"));
        assertThat(query.getLowerTerm(), equalTo(new BytesRef(pointer.serialize())));
        assertThat(query.includesLower(), is(false));
        assertThat(query.getUpperTerm(), is(nullValue()));
        assertThat(query.includesUpper(), is(true));
    }

    public void testEqualsSameInstance() {
        ObjectStorePointer pointer = new ObjectStorePointer("k", 0);
        assertThat(pointer.equals(pointer), is(true));
    }

    public void testEqualsNull() {
        assertThat(new ObjectStorePointer("k", 0).equals(null), is(false));
    }

    public void testEqualsDifferentType() {
        assertThat(new ObjectStorePointer("k", 0).equals("k"), is(false));
    }

    public void testNotEqualsDifferingFields() {
        ObjectStorePointer base = new ObjectStorePointer("k", 1);
        assertThat(base, not(equalTo(new ObjectStorePointer("k", 2))));
        assertThat(base, not(equalTo(new ObjectStorePointer("other", 1))));
    }

    public void testHashCodeConsistentForEqual() {
        assertThat(new ObjectStorePointer("k", 1).hashCode(), is(new ObjectStorePointer("k", 1).hashCode()));
    }

    public void testToStringContainsFields() {
        String s = new ObjectStorePointer("k", 3).toString();
        assertThat(s, containsString("objectKey='k'"));
        assertThat(s, containsString("byteOffset=3"));
    }
}

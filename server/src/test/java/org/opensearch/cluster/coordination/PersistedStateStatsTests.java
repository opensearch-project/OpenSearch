/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class PersistedStateStatsTests extends OpenSearchTestCase {
    private PersistedStateStats persistedStateStats;

    @Before
    public void setup() {
        persistedStateStats = new PersistedStateStats("testStats");
    }

    public void testAddToExtendedFieldsNewField() {
        String fieldName = "testField";
        AtomicLong fieldValue = new AtomicLong(42);

        persistedStateStats.addToExtendedFields(fieldName, fieldValue);

        assertTrue(persistedStateStats.getExtendedFields().containsKey(fieldName));
        assertEquals(42, persistedStateStats.getExtendedFields().get(fieldName).get());
    }

    public void testAddToExtendedFieldsExistingField() {
        String fieldName = "testField";
        AtomicLong initialValue = new AtomicLong(42);
        persistedStateStats.addToExtendedFields(fieldName, initialValue);

        AtomicLong newValue = new AtomicLong(84);
        persistedStateStats.addToExtendedFields(fieldName, newValue);

        assertTrue(persistedStateStats.getExtendedFields().containsKey(fieldName));
        assertEquals(84, persistedStateStats.getExtendedFields().get(fieldName).get());
    }

    public void testAddMultipleFields() {
        String fieldName1 = "testField1";
        AtomicLong fieldValue1 = new AtomicLong(42);

        String fieldName2 = "testField2";
        AtomicLong fieldValue2 = new AtomicLong(84);

        persistedStateStats.addToExtendedFields(fieldName1, fieldValue1);
        persistedStateStats.addToExtendedFields(fieldName2, fieldValue2);

        assertTrue(persistedStateStats.getExtendedFields().containsKey(fieldName1));
        assertTrue(persistedStateStats.getExtendedFields().containsKey(fieldName2));

        assertEquals(42, persistedStateStats.getExtendedFields().get(fieldName1).get());
        assertEquals(84, persistedStateStats.getExtendedFields().get(fieldName2).get());
    }

    public void testBwcSerializationWithExtraExtendedField() throws IOException {
        // Simulate NEW node writing stats with 3 extended fields
        PersistedStateStats newNodeStats = new PersistedStateStats("test_download");
        newNodeStats.addToExtendedFields("checksum_validation_failed_count", new AtomicLong(0));
        newNodeStats.addToExtendedFields("incoming_publication_failed_count", new AtomicLong(1));
        newNodeStats.addToExtendedFields("current_application_duration_ms", new AtomicLong(42));

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        newNodeStats.writeTo(out);

        // Deserialize as generic PersistedStateStats (simulates OLD node reading)
        StreamInput in = out.bytes().streamInput();
        PersistedStateStats oldNodeDeserialized = new PersistedStateStats(in);

        // Old node reads all 3 fields successfully — no bytes left unread
        assertEquals(3, oldNodeDeserialized.getExtendedFields().size());
        assertEquals(42, oldNodeDeserialized.getExtendedFields().get("current_application_duration_ms").get());

        // Simulate OLD node writing stats with only 2 extended fields
        PersistedStateStats oldNodeStats = new PersistedStateStats("test_download");
        oldNodeStats.addToExtendedFields("checksum_validation_failed_count", new AtomicLong(0));
        oldNodeStats.addToExtendedFields("incoming_publication_failed_count", new AtomicLong(1));
        // Note: NO current_application_duration_ms

        BytesStreamOutput out2 = new BytesStreamOutput();
        oldNodeStats.writeTo(out2);

        StreamInput in2 = out2.bytes().streamInput();
        PersistedStateStats newNodeDeserialized = new PersistedStateStats(in2);

        // New node reads only 2 fields — current_application_duration_ms simply absent
        assertEquals(2, newNodeDeserialized.getExtendedFields().size());
        assertFalse(newNodeDeserialized.getExtendedFields().containsKey("current_application_duration_ms"));
    }

    // serialization with extendedFields
    public void testSerializationRoundTripWithExtendedFields() throws IOException {
        PersistedStateStats original = new PersistedStateStats("test_download");
        original.stateSucceeded();
        original.stateTook(100);
        original.addToExtendedFields("current_application_duration_ms", new AtomicLong(5000));

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        PersistedStateStats deserialized = new PersistedStateStats(in);

        assertEquals("test_download", deserialized.getStatsName());
        assertEquals(1, deserialized.getSuccessCount());
        assertEquals(100, deserialized.getTotalTimeInMillis());
        assertTrue(deserialized.getExtendedFields().containsKey("current_application_duration_ms"));
        assertEquals(5000, deserialized.getExtendedFields().get("current_application_duration_ms").get());
    }

}

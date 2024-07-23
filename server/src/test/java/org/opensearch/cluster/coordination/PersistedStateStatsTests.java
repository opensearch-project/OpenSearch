/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

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
}

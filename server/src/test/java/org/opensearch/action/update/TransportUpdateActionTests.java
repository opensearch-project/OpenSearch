/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.update;

import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;

public class TransportUpdateActionTests extends OpenSearchTestCase {

    /**
     * Test that TransportUpdateAction has TransportShardBulkAction as a field.
     * This ensures the refactoring to use TransportShardBulkAction directly is maintained.
     */
    public void testTransportUpdateActionHasShardBulkActionField() throws NoSuchFieldException {
        // Verify that TransportUpdateAction has a private field for TransportShardBulkAction
        Field shardBulkActionField = TransportUpdateAction.class.getDeclaredField("shardBulkAction");
        
        // Verify the field exists and has the correct type
        assertNotNull(shardBulkActionField);
        assertEquals(TransportShardBulkAction.class, shardBulkActionField.getType());
        
        // Verify it's private (implementation detail check)
        assertTrue(java.lang.reflect.Modifier.isPrivate(shardBulkActionField.getModifiers()));
    }
}
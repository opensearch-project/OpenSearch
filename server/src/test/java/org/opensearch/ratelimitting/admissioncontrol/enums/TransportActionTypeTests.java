/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.enums;

import org.opensearch.test.OpenSearchTestCase;

public class TransportActionTypeTests extends OpenSearchTestCase {

    public void testValidActionType() {
        assertEquals(TransportActionType.SEARCH.getType(), "search");
        assertEquals(TransportActionType.INDEXING.getType(), "indexing");
        assertEquals(TransportActionType.fromName("search"), TransportActionType.SEARCH);
        assertEquals(TransportActionType.fromName("indexing"), TransportActionType.INDEXING);
    }

    public void testInValidActionType() {
        String name = "test";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> TransportActionType.fromName(name));
        assertEquals(ex.getMessage(), "Not Supported TransportAction Type: " + name);
    }
}

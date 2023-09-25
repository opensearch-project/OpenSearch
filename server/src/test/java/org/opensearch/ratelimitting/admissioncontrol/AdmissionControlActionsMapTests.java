/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.ratelimitting.admissioncontrol.enums.TransportActionType;
import org.opensearch.test.OpenSearchTestCase;

public class AdmissionControlActionsMapTests extends OpenSearchTestCase {

    public void testDefaultActions() {
        assertEquals(AdmissionControlActionsMap.transportActionsMap.size(), 5);
        String action = BulkAction.NAME + "[s][p]";
        assertTrue(AdmissionControlActionsMap.containsActionType(action));
        assertEquals(AdmissionControlActionsMap.getTransportActionType(action), TransportActionType.INDEXING.getType());
        action = SearchTransportService.QUERY_ACTION_NAME;
        assertTrue(AdmissionControlActionsMap.containsActionType(action));
        assertEquals(AdmissionControlActionsMap.getTransportActionType(action), TransportActionType.SEARCH.getType());
        action = SearchTransportService.DFS_ACTION_NAME;
        assertTrue(AdmissionControlActionsMap.containsActionType(action));
        assertEquals(AdmissionControlActionsMap.getTransportActionType(action), TransportActionType.SEARCH.getType());
        action = SearchTransportService.FETCH_ID_ACTION_NAME;
        assertTrue(AdmissionControlActionsMap.containsActionType(action));
        assertEquals(AdmissionControlActionsMap.getTransportActionType(action), TransportActionType.SEARCH.getType());
        action = SearchTransportService.QUERY_ID_ACTION_NAME;
        assertTrue(AdmissionControlActionsMap.containsActionType(action));
        assertEquals(AdmissionControlActionsMap.getTransportActionType(action), TransportActionType.SEARCH.getType());
    }

    public void testInvalidActions() {
        String action = "TEST";
        assertFalse(AdmissionControlActionsMap.containsActionType(action));
        assertNull(AdmissionControlActionsMap.getTransportActionType(action));
    }
}

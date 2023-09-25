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

import java.util.HashMap;
import java.util.Map;

/**
 * Map to store the actions based on the type of the transport operation
 */
public class AdmissionControlActionsMap {
    public static Map<String, String> transportActionsMap;

    // Adding below as actions will add/update more actions based on discussions in next PR's.
    static {
        transportActionsMap = new HashMap<>();
        transportActionsMap.put(BulkAction.NAME + "[s][p]", TransportActionType.INDEXING.getType());
        transportActionsMap.put(SearchTransportService.QUERY_ACTION_NAME, TransportActionType.SEARCH.getType());
        transportActionsMap.put(SearchTransportService.QUERY_ID_ACTION_NAME, TransportActionType.SEARCH.getType());
        transportActionsMap.put(SearchTransportService.DFS_ACTION_NAME, TransportActionType.SEARCH.getType());
        transportActionsMap.put(SearchTransportService.FETCH_ID_ACTION_NAME, TransportActionType.SEARCH.getType());
    }

    /**
     *
     * @param transportAction type of the transport action
     * @return list of the actions based on the type
     */
    public static String getTransportActionType(String transportAction) {
        return transportActionsMap.getOrDefault(transportAction, null);
    }

    public static Boolean containsActionType(String transportAction) {
        return transportActionsMap.containsKey(transportAction);
    }
}

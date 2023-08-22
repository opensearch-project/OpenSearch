/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol;

import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.throttling.admissioncontrol.enums.TransportActionType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Map to store the actions based on the type of the transport operation
 */
public class AdmissionControlActionsMap {
    public static Map<String, List<String>> transportActionsMap;

    // Adding below as actions will add/update more actions based on discussions in next PR's.
    static {
        transportActionsMap = new HashMap<>();
        transportActionsMap.put(
            TransportActionType.INDEXING.getType(),
            Arrays.asList(BulkAction.NAME + "[s][p]", BulkAction.NAME + "[s][r]")
        );
        transportActionsMap.put(
            TransportActionType.SEARCH.getType(),
            Arrays.asList(
                SearchTransportService.QUERY_ACTION_NAME,
                SearchTransportService.QUERY_ID_ACTION_NAME,
                SearchTransportService.DFS_ACTION_NAME,
                SearchTransportService.FETCH_ID_ACTION_NAME
            )
        );
    }

    /**
     *
     * @param transportAction type of the transport action
     * @return list of the actions based on the type
     */
    public static List<String> getTransportActionType(String transportAction) {
        // if the transport action is not present in map then considering that as separate action
        // TODO before returning we can evaluate if the action is valid or not if it was not present in map
        return transportActionsMap.getOrDefault(transportAction, Collections.singletonList(transportAction));
    }
}

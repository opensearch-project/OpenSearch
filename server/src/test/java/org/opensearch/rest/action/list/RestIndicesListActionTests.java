/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.list;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.pagination.IndexPaginationStrategy;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.breaker.ResponseLimitSettings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;

public class RestIndicesListActionTests extends OpenSearchTestCase {

    private RestIndicesListAction createAction() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, Settings.EMPTY);
        return new RestIndicesListAction(responseLimitSettings);
    }

    public void testGetPaginationStrategyFiltersByGetSettingsResponse() {
        // Build cluster state with 3 regular indices and 2 system indices
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .build();
        clusterState = addIndex(clusterState, "test-index-1", 1);
        clusterState = addIndex(clusterState, "test-index-2", 2);
        clusterState = addIndex(clusterState, "test-index-3", 3);
        clusterState = addIndex(clusterState, ".opendistro_security", 4);
        clusterState = addIndex(clusterState, ".system-index", 5);

        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(new ClusterName("test"), clusterState, false);

        // GetSettingsResponse only contains authorized indices (simulating security plugin filtering)
        Map<String, Settings> authorizedSettings = new HashMap<>();
        authorizedSettings.put("test-index-1", Settings.EMPTY);
        authorizedSettings.put("test-index-2", Settings.EMPTY);
        authorizedSettings.put("test-index-3", Settings.EMPTY);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(authorizedSettings, Collections.emptyMap());

        // Set pageParams and call getPaginationStrategy
        RestIndicesListAction action = createAction();
        action.pageParams = new PageParams(null, PARAM_ASC_SORT_VALUE, 10);

        IndexPaginationStrategy strategy = action.getPaginationStrategy(clusterStateResponse, getSettingsResponse);

        // Only authorized indices should be returned, system indices should be filtered out
        assertNotNull(strategy);
        assertEquals(3, strategy.getRequestedEntities().size());
        assertTrue(strategy.getRequestedEntities().contains("test-index-1"));
        assertTrue(strategy.getRequestedEntities().contains("test-index-2"));
        assertTrue(strategy.getRequestedEntities().contains("test-index-3"));
        assertFalse(strategy.getRequestedEntities().contains(".opendistro_security"));
        assertFalse(strategy.getRequestedEntities().contains(".system-index"));
        // All indices fit in one page, so no next token
        assertNull(strategy.getResponseToken().getNextToken());
    }

    public void testGetPaginationStrategyPaginatesFilteredIndicesCorrectly() {
        // Build cluster state with 5 regular indices and 2 system indices
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .build();
        clusterState = addIndex(clusterState, "test-index-1", 1);
        clusterState = addIndex(clusterState, "test-index-2", 2);
        clusterState = addIndex(clusterState, ".opendistro_security", 3);
        clusterState = addIndex(clusterState, "test-index-3", 4);
        clusterState = addIndex(clusterState, ".system-index", 5);
        clusterState = addIndex(clusterState, "test-index-4", 6);
        clusterState = addIndex(clusterState, "test-index-5", 7);

        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(new ClusterName("test"), clusterState, false);

        Map<String, Settings> authorizedSettings = new HashMap<>();
        authorizedSettings.put("test-index-1", Settings.EMPTY);
        authorizedSettings.put("test-index-2", Settings.EMPTY);
        authorizedSettings.put("test-index-3", Settings.EMPTY);
        authorizedSettings.put("test-index-4", Settings.EMPTY);
        authorizedSettings.put("test-index-5", Settings.EMPTY);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(authorizedSettings, Collections.emptyMap());

        RestIndicesListAction action = createAction();

        // First page: size=2
        action.pageParams = new PageParams(null, PARAM_ASC_SORT_VALUE, 2);
        IndexPaginationStrategy strategy = action.getPaginationStrategy(clusterStateResponse, getSettingsResponse);

        assertEquals(2, strategy.getRequestedEntities().size());
        assertEquals("test-index-1", strategy.getRequestedEntities().get(0));
        assertEquals("test-index-2", strategy.getRequestedEntities().get(1));
        assertNotNull(strategy.getResponseToken().getNextToken());

        // Second page
        action.pageParams = new PageParams(strategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 2);
        strategy = action.getPaginationStrategy(clusterStateResponse, getSettingsResponse);

        assertEquals(2, strategy.getRequestedEntities().size());
        assertEquals("test-index-3", strategy.getRequestedEntities().get(0));
        assertEquals("test-index-4", strategy.getRequestedEntities().get(1));
        assertNotNull(strategy.getResponseToken().getNextToken());

        // Third page (last)
        action.pageParams = new PageParams(strategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 2);
        strategy = action.getPaginationStrategy(clusterStateResponse, getSettingsResponse);

        assertEquals(1, strategy.getRequestedEntities().size());
        assertEquals("test-index-5", strategy.getRequestedEntities().get(0));
        assertNull(strategy.getResponseToken().getNextToken());
    }

    private ClusterState addIndex(ClusterState clusterState, String indexName, int creationOrder) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                settings(Version.CURRENT).put(SETTING_CREATION_DATE, Instant.now().plus(creationOrder, ChronoUnit.SECONDS).toEpochMilli())
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexRoutingTable.Builder indexRoutingTableBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex());
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder(clusterState.routingTable()).add(indexRoutingTableBuilder).build())
            .build();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchTransportServiceTests extends OpenSearchTestCase {

    public void testDfsQueryPhaseRecordsAdaptiveReplicaSelectionStats() {
        final ResponseCollectorService collector = new ResponseCollectorService(mock(ClusterService.class));
        final TransportService transportService = mock(TransportService.class);
        final SearchTransportService searchTransportService = new SearchTransportService(
            transportService,
            SearchExecutionStatsCollector.makeWrapper(collector)
        );

        final DiscoveryNode node = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        final Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getNode()).thenReturn(node);

        final SearchShardTarget target = new SearchShardTarget("node_1", new ShardId("test", "na", 0), null, OriginalIndices.NONE);
        final AtomicReference<QuerySearchResult> received = new AtomicReference<>();
        final SearchActionListener<QuerySearchResult> listener = new SearchActionListener<QuerySearchResult>(target, 0) {
            @Override
            protected void innerOnResponse(QuerySearchResult response) {
                received.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };

        searchTransportService.sendExecuteQuery(connection, mock(QuerySearchRequest.class), mock(SearchTask.class), listener);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<TransportResponseHandler<QuerySearchResult>> handlerCaptor = ArgumentCaptor.forClass(
            TransportResponseHandler.class
        );
        verify(transportService).sendChildRequest(
            eq(connection),
            eq(SearchTransportService.QUERY_ID_ACTION_NAME),
            any(),
            any(),
            handlerCaptor.capture()
        );

        assertNull(collector.getNodeStatistics("node_1").orElse(null));
        final QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", 1), target, null);
        result.setSearchShardTarget(target);
        result.setShardIndex(0);
        result.serviceTimeEWMA(1_000_000L);
        result.nodeQueueSize(2);
        handlerCaptor.getValue().handleResponse(result);

        assertSame(result, received.get());
        assertTrue(collector.getNodeStatistics("node_1").isPresent());
    }
}

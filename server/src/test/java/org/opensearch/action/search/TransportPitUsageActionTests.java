/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPitUsageActionTests extends OpenSearchTestCase {

    private TransportPitUsageAction createAction() {
        ClusterService clusterService = mock(ClusterService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
            )
        );
        NodeClient client = mock(NodeClient.class);
        when(client.getNamedWriteableRegistry()).thenReturn(namedWriteableRegistry);
        PitService pitService = new PitService(clusterService, mock(SearchTransportService.class), mock(TransportService.class), client);

        return new TransportPitUsageAction(
            clusterService,
            mock(TransportService.class),
            mock(ActionFilters.class),
            indexNameExpressionResolver,
            mock(SearchService.class),
            namedWriteableRegistry,
            pitService
        );
    }

    public void testResolveIndices() {
        TransportPitUsageAction action = createAction();

        {
            // The generated pitId will contain the indices "idx" and "idy"
            String pitId = PitTestsUtil.getPitId();
            OptionallyResolvedIndices resolvedIndices = action.resolveIndices(new PitUsageRequest(pitId));
            assertEquals(ResolvedIndices.of("idx", "idy"), resolvedIndices);
        }

        {
            OptionallyResolvedIndices resolvedIndices = action.resolveIndices(new PitUsageRequest("_all"));
            assertFalse("result is unknown", resolvedIndices instanceof ResolvedIndices);
        }
    }

    public void testNewResponseAggregatesShardUsagePerPit() {
        TransportPitUsageAction action = createAction();
        List<PitUsageInfo> shardResults = Arrays.asList(
            new PitUsageInfo("pit1", 3, 10),
            new PitUsageInfo("pit1", 3, 20),
            new PitUsageInfo("pit1", 1, 0),
            new PitUsageInfo("pit2", 5, 7)
        );

        PitUsageResponse response = action.newResponse(
            new PitUsageRequest("pit1", "pit2"),
            4,
            4,
            0,
            shardResults,
            Collections.emptyList(),
            ClusterState.EMPTY_STATE
        );

        assertEquals(4, response.getTotalShards());
        assertEquals(Arrays.asList(new PitUsageInfo("pit1", 3, 30), new PitUsageInfo("pit2", 5, 7)), response.getPitUsages());
    }

    public void testNewResponseWithNoShardResults() {
        PitUsageResponse response = createAction().newResponse(
            new PitUsageRequest("pit1"),
            0,
            0,
            0,
            Collections.emptyList(),
            Collections.emptyList(),
            ClusterState.EMPTY_STATE
        );

        assertTrue(response.getPitUsages().isEmpty());
    }
}

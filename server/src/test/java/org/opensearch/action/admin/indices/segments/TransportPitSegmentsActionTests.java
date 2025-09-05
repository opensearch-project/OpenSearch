/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.search.PitService;
import org.opensearch.action.search.PitTestsUtil;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.action.support.ActionFilters;
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
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPitSegmentsActionTests extends OpenSearchTestCase {
    public void testResolveIndices() {
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

        TransportPitSegmentsAction action = new TransportPitSegmentsAction(
            clusterService,
            mock(TransportService.class),
            mock(IndicesService.class),
            mock(ActionFilters.class),
            indexNameExpressionResolver,
            mock(SearchService.class),
            namedWriteableRegistry,
            pitService
        );

        {
            // The generated pitId will contain the indices "idx" and "idy"
            String pitId = PitTestsUtil.getPitId();
            OptionallyResolvedIndices resolvedIndices = action.resolveIndices(new PitSegmentsRequest(pitId));
            assertEquals(ResolvedIndices.of("idx", "idy"), resolvedIndices);
        }

        {
            OptionallyResolvedIndices resolvedIndices = action.resolveIndices(new PitSegmentsRequest("_all"));
            assertFalse("result is unknown", resolvedIndices instanceof ResolvedIndices);
        }
    }
}

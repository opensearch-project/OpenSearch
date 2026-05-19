/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.List;

import static org.opensearch.cluster.DataStreamTestHelper.createBackingIndex;
import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStreamStatsActionTests extends OpenSearchTestCase {
    public void testResolveIndices() throws Exception {
        String dataStreamName = "datastream-test";

        IndexMetadata index1 = createBackingIndex(dataStreamName, 1).build();
        IndexMetadata index2 = createBackingIndex(dataStreamName, 2).build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(index1.getIndex(), index2.getIndex()), 2));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        DataStreamsStatsAction.TransportAction action = new DataStreamsStatsAction.TransportAction(
            clusterService,
            mock(TransportService.class),
            mock(IndicesService.class),
            mock(ActionFilters.class),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );

        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new DataStreamsStatsAction.Request().indices("datastream-*"));
            assertEquals(ResolvedIndices.of("datastream-test"), resolvedIndices);
        }

        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new DataStreamsStatsAction.Request());
            assertEquals(ResolvedIndices.of("datastream-test"), resolvedIndices);
        }
    }
}

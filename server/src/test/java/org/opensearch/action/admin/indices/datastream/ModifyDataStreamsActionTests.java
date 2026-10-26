/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataDataStreamsService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.List;

import static org.mockito.Mockito.mock;

public class ModifyDataStreamsActionTests extends OpenSearchTestCase {

    public void testResolveIndicesReturnsEveryDataStreamAndBackingIndex() {
        ModifyDataStreamsAction.TransportAction action = new ModifyDataStreamsAction.TransportAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            mock(MetadataDataStreamsService.class)
        );

        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(
            List.of(
                DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"),
                DataStreamAction.removeBackingIndex("logs-bar", ".ds-logs-bar-000002")
            )
        );

        ResolvedIndices resolvedIndices = action.resolveIndices(request);
        assertEquals(ResolvedIndices.of("logs-foo", ".ds-logs-foo-000001", "logs-bar", ".ds-logs-bar-000002"), resolvedIndices);
    }
}

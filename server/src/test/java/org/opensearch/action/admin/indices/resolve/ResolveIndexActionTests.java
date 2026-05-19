/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.resolve;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterConnectionTests;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResolveIndexActionTests extends OpenSearchTestCase {
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testResolveIndices() {
        IndexMetadata.Builder indexA1 = IndexMetadata.builder("index_a1")
            .putAlias(AliasMetadata.builder("alias_a").build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        IndexMetadata.Builder indexA2 = IndexMetadata.builder("index_a2")
            .putAlias(AliasMetadata.builder("alias_a").build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexA1).put(indexA2))
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        Settings.Builder settingsBuilder = Settings.builder();
        try (
            MockTransportService remoteSeedTransport = RemoteClusterConnectionTests.startTransport(
                "node_remote",
                Collections.emptyList(),
                Version.CURRENT,
                threadPool
            )
        ) {
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            settingsBuilder.put("cluster.remote.remote1.seeds", remoteSeedNode.getAddress().toString());

            try (
                MockTransportService transportService = MockTransportService.createNewService(
                    settingsBuilder.build(),
                    Version.CURRENT,
                    threadPool,
                    NoopTracer.INSTANCE
                )
            ) {
                transportService.start();
                transportService.acceptIncomingRequests();

                ResolveIndexAction.TransportAction action = new ResolveIndexAction.TransportAction(
                    transportService,
                    clusterService,
                    threadPool,
                    mock(ActionFilters.class),
                    indexNameExpressionResolver
                );

                // Actual test cases start here:
                {
                    ResolvedIndices resolvedIndices = action.resolveIndices(new ResolveIndexAction.Request(new String[] { "index_*" }));
                    assertEquals(ResolvedIndices.of("index_a1", "index_a2"), resolvedIndices);
                }

                {
                    ResolvedIndices resolvedIndices = action.resolveIndices(new ResolveIndexAction.Request(new String[] { "alias_a" }));
                    assertEquals(ResolvedIndices.of("alias_a"), resolvedIndices);
                }

                {
                    ResolvedIndices resolvedIndices = action.resolveIndices(
                        new ResolveIndexAction.Request(new String[] { "index_non_existing" })
                    );
                    assertEquals(ResolvedIndices.of("index_non_existing"), resolvedIndices);
                }

                {
                    ResolvedIndices resolvedIndices = action.resolveIndices(
                        new ResolveIndexAction.Request(new String[] { "index_*", "remote1:remote_index" })
                    );
                    assertEquals(
                        ResolvedIndices.of("index_a1", "index_a2")
                            .withRemoteIndices(
                                Map.of("remote1", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN))
                            ),
                        resolvedIndices
                    );
                }

            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the security-wiring contract for {@link AnalyticsPlugin.DefaultEngineContextProvider}:
 * {@code getContext()} must thread the cluster's {@link IndexNameExpressionResolver} (which
 * carries security-plugin extensions and system-index access rules) into
 * {@code OpenSearchSchemaBuilder.buildSchema}, not silently construct a fresh resolver. A
 * regression to the single-arg buildSchema(state) overload would bypass those checks.
 */
public class DefaultEngineContextProviderTests extends OpenSearchTestCase {

    public void testGetContextUsesInjectedResolver() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        IndexNameExpressionResolver injectedResolver = mock(IndexNameExpressionResolver.class);
        when(
            injectedResolver.concreteIndexNames(
                same(clusterState),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[0]);

        AnalyticsPlugin.DefaultEngineContextProvider ctx = new AnalyticsPlugin.DefaultEngineContextProvider(
            clusterService,
            injectedResolver,
            null
        );

        SchemaPlus schema = ctx.getContext().schema();
        // Trigger a lazy resolve so the resolver is actually invoked. Cluster state has no
        // indices, so the lookup returns null — but the resolver gets called along the way
        // (in IndexResolution.resolve's fallback path), which is what we're pinning.
        schema.getTable("any_table");

        verify(injectedResolver, atLeastOnce()).concreteIndexNames(
            same(clusterState),
            any(IndicesOptions.class),
            org.mockito.ArgumentMatchers.anyBoolean(),
            any(String[].class)
        );
    }
}

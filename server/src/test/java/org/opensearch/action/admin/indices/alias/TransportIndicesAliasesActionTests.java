/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.alias;

import org.opensearch.Version;
import org.opensearch.action.RequestValidators;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexAliasesService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportIndicesAliasesActionTests extends OpenSearchTestCase {
    private static ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportIndicesAliasesAction subject;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("index_a1").putAlias(AliasMetadata.builder("alias_a")))
            .put(indexBuilder("index_a2").putAlias(AliasMetadata.builder("alias_a")))
            .put(indexBuilder("index_b1"))
            .put(indexBuilder("index_b2"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        this.clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        this.subject = new TransportIndicesAliasesAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(MetadataIndexAliasesService.class),
            mock(ActionFilters.class),
            new IndexNameExpressionResolver(threadPool.getThreadContext()),
            new RequestValidators<>(List.of())
        );
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testResolvedIndices_addAliasAction() {
        ResolvedIndices resolvedIndices = subject.resolveIndices(
            new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index_b*").alias("alias_b"))
        );
        assertEquals(ResolvedIndices.of("index_b1", "index_b2", "alias_b"), resolvedIndices);
    }

    public void testResolvedIndices_removeAliasAction() {
        ResolvedIndices resolvedIndices = subject.resolveIndices(
            new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("index_a*").alias("alias_*"))
        );
        assertEquals(ResolvedIndices.of("index_a1", "index_a2", "alias_a"), resolvedIndices);
    }

    public void testResolvedIndices_removeIndexAction() {
        ResolvedIndices resolvedIndices = subject.resolveIndices(
            new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("index_a*"))
        );
        assertEquals(
            ResolvedIndices.of(Collections.emptyList())
                .withLocalSubActions(DeleteIndexAction.INSTANCE, ResolvedIndices.Local.of("index_a1", "index_a2")),
            resolvedIndices
        );
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
    }

}

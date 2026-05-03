/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.action.admin.cluster.catalog.GetPublishStatusAction;
import org.opensearch.action.admin.cluster.catalog.GetPublishStatusRequest;
import org.opensearch.action.admin.cluster.catalog.GetPublishStatusResponse;
import org.opensearch.action.admin.cluster.catalog.PublishIndexAction;
import org.opensearch.action.admin.cluster.catalog.PublishIndexRequest;
import org.opensearch.action.admin.cluster.catalog.PublishIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;

/**
 * End-to-end submit → state-machine → rollback → entry-removed. The test index has no
 * remote segment store configured, so {@code copyShard} fails; the state machine routes
 * through {@code FINALIZING_FAILURE} and the mock records a {@code finalizePublish(false)}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class AsyncSubmitAndPollIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockCatalogPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.CATALOG_REPOSITORY_TYPE_SETTING.getKey(), MockCatalogPlugin.TYPE)
            .build();
    }

    public void testSubmitAndPollEventuallyRemovesEntry() throws Exception {
        String indexName = "catalog-it-" + randomAlphaOfLength(5).toLowerCase();

        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen(indexName);

        PublishIndexResponse submitResponse = client()
            .execute(PublishIndexAction.INSTANCE, new PublishIndexRequest(indexName))
            .actionGet();
        assertTrue(submitResponse.isAcknowledged());
        String publishId = submitResponse.publishId();
        assertNotNull(publishId);

        assertBusy(() -> {
            GetPublishStatusResponse status = client()
                .execute(GetPublishStatusAction.INSTANCE, new GetPublishStatusRequest(null, publishId))
                .actionGet();
            assertEquals(GetPublishStatusResponse.Status.NOT_FOUND, status.status());
        });

        MockCatalogPlugin plugin = internalCluster().getInstance(org.opensearch.plugins.PluginsService.class)
            .filterPlugins(MockCatalogPlugin.class)
            .iterator().next();
        MockCatalogMetadataClient mock = plugin.getClient();
        assertNotNull(mock.indexToFinalizeCalls().get(indexName));
        assertFalse(
            mock.indexToFinalizeCalls().get(indexName).stream().anyMatch(MockCatalogMetadataClient.FinalizeCall::success)
        );
    }

    public void testSecondSubmitForSameIndexIsRejected() throws Exception {
        String indexName = "catalog-it-dup-" + randomAlphaOfLength(5).toLowerCase();

        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen(indexName);

        client().execute(PublishIndexAction.INSTANCE, new PublishIndexRequest(indexName)).actionGet();

        try {
            client().execute(PublishIndexAction.INSTANCE, new PublishIndexRequest(indexName)).actionGet();
            fail("second submit for same index should have been rejected");
        } catch (Exception e) {
            Throwable cause = e.getCause();
            assertTrue(
                "expected IllegalStateException, got " + (cause == null ? e : cause),
                (cause != null ? cause : e) instanceof IllegalStateException
            );
        }
    }

    public void testStatusNotFoundForUnknownPublishId() {
        GetPublishStatusResponse status = client()
            .execute(GetPublishStatusAction.INSTANCE, new GetPublishStatusRequest(null, "does-not-exist"))
            .actionGet();
        assertEquals(GetPublishStatusResponse.Status.NOT_FOUND, status.status());
        assertNull(status.entry());
    }
}

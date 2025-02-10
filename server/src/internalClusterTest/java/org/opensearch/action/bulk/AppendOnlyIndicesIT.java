/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.bulk;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ingest.IngestTestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;

public class AppendOnlyIndicesIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestTestPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testIndexDocumentWithACustomDocIdForAppendOnlyIndices() throws Exception {
        Client client = internalCluster().coordOnlyNodeClient();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index")
                .setSettings(
                    Settings.builder()
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                )
        );
        ensureGreen("index");

        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        XContentBuilder doc = null;
        doc = jsonBuilder().startObject().field("foo", "bar").endObject();
        bulkBuilder.add(client.prepareIndex("index").setId(Integer.toString(0)).setSource(doc));

        BulkResponse response = bulkBuilder.get();
        assertThat(
            response.getItems()[0].getFailureMessage(),
            containsString(
                "Operation [INDEX] is not allowed with a custom document id 0 as setting `"
                    + IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey()
                    + "` is enabled for this index: index;"
            )
        );
    }

    public void testUpdateDeleteDocumentForAppendOnlyIndices() throws Exception {
        Client client = internalCluster().coordOnlyNodeClient();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index")
                .setSettings(
                    Settings.builder()
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                )
        );
        ensureGreen("index");

        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        XContentBuilder doc = null;
        doc = jsonBuilder().startObject().field("foo", "bar").endObject();
        bulkBuilder.add(client.prepareIndex("index").setSource(doc));

        bulkBuilder.get();
        BulkResponse response = client().prepareBulk().add(client().prepareUpdate("index", "0").setDoc("foo", "updated")).get();
        assertThat(
            response.getItems()[0].getFailureMessage(),
            containsString(
                "Operation [UPDATE] is not allowed as setting `"
                    + IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey()
                    + "` is enabled for this index"
            )
        );

        response = client().prepareBulk().add(client().prepareDelete("index", "0")).get();
        assertThat(
            response.getItems()[0].getFailureMessage(),
            containsString(
                "Operation [DELETE] is not allowed as setting `"
                    + IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey()
                    + "` is enabled for this index"
            )
        );
    }

    public void testRetryForAppendOnlyIndices() throws Exception {
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        int numDocs = scaledRandomIntBetween(100, 1000);
        Client client = internalCluster().coordOnlyNodeClient();
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        NodeStats unluckyNode = randomFrom(
            nodeStats.getNodes().stream().filter((s) -> s.getNode().isDataNode()).collect(Collectors.toList())
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index")
                .setSettings(
                    Settings.builder()
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                )
        );
        ensureGreen("index");
        logger.info("unlucky node: {}", unluckyNode.getNode());
        // create a transport service that throws a ConnectTransportException for one bulk request and therefore triggers a retry.
        for (NodeStats dataNode : nodeStats.getNodes()) {
            if (exceptionThrown.get()) {
                break;
            }

            MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
                TransportService.class,
                dataNode.getNode().getName()
            ));
            mockTransportService.addSendBehavior(
                internalCluster().getInstance(TransportService.class, unluckyNode.getNode().getName()),
                (connection, requestId, action, request, options) -> {
                    connection.sendRequest(requestId, action, request, options);
                    if (action.equals(TransportShardBulkAction.ACTION_NAME) && exceptionThrown.compareAndSet(false, true)) {
                        logger.debug("Throw ConnectTransportException");
                        throw new ConnectTransportException(connection.getNode(), action);
                    }
                }
            );
        }

        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        for (int i = 0; i < numDocs; i++) {
            XContentBuilder doc = null;
            doc = jsonBuilder().startObject().field("foo", "bar").endObject();
            bulkBuilder.add(client.prepareIndex("index").setSource(doc));
        }

        BulkResponse response = bulkBuilder.get();
        for (BulkItemResponse singleIndexResponse : response.getItems()) {
            // Retry will not create a new version.
            assertThat(singleIndexResponse.getVersion(), equalTo(1L));
        }
    }

    public void testNodeReboot() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        Client client = internalCluster().coordOnlyNodeClient();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index")
                .setSettings(
                    Settings.builder()
                        .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                        .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                )
        );

        ensureGreen("index");

        BulkRequestBuilder bulkBuilder = client.prepareBulk();

        for (int i = 0; i < numDocs; i++) {
            XContentBuilder doc = null;
            doc = jsonBuilder().startObject().field("foo", "bar").endObject();
            bulkBuilder.add(client.prepareIndex("index").setSource(doc));
        }

        BulkResponse response = bulkBuilder.get();
        assertFalse(response.hasFailures());
        internalCluster().restartRandomDataNode();
        ensureGreen("index");
        refresh();
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchAllQuery())
            .setIndices("index")
            .setSize(numDocs)
            .get();

        assertBusy(() -> { assertHitCount(searchResponse, numDocs); }, 20L, TimeUnit.SECONDS);

    }
}

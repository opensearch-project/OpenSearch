/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.plugin.ArrowBasePlugin;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 3, maxNumDataNodes = 3)
public class FlightTransportIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                FlightStreamPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                FlightStreamPlugin.class.getName(),
                null,
                List.of(ArrowBasePlugin.class.getName()),
                false
            )
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(3);
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 3)    // Number of primary shards
            .put("index.number_of_replicas", 0)  // Number of replica shards
            .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("index").settings(indexSettings);
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());
        client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();
        BulkRequest bulkRequest = new BulkRequest();

        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 42));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 43));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 44));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 42));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 43));
        bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 44));

        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures()); // Verify ingestion was successful
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();
        ensureSearchable("index");
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testArrowFlightProducer() throws Exception {
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index").execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(3, resp.getTotalShards());
        assertEquals(6, resp.getHits().getTotalHits().value());
        for (SearchHit hit : resp.getHits().getHits()) {
            assertNotNull(hit.getSourceAsString());
        }
    }
}

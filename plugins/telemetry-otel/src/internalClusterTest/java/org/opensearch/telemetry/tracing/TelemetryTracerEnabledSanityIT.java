/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.telemetry.tracing.TelemetryValidators;
import org.opensearch.test.telemetry.tracing.validators.AllSpansAreEndedProperly;
import org.opensearch.test.telemetry.tracing.validators.AllSpansHaveUniqueId;
import org.opensearch.test.telemetry.tracing.validators.NumberOfTraceIDsEqualToRequests;
import org.opensearch.test.telemetry.tracing.validators.TotalRootSpansEqualToRequests;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class TelemetryTracerEnabledSanityIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.tracing.InMemorySingletonSpanExporter"
            )
            .put(OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(TelemetrySettings.TRACER_SAMPLER_PROBABILITY.getKey(), 1.0d)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IntegrationTestOTelTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockTelemetryPlugin() {
        return false;
    }

    public void testSanityChecksWhenTracingEnabled() throws Exception {
        Client client = internalCluster().clusterManagerClient();
        // ENABLE TRACING
        updateTelemetrySetting(client, true);

        // Create Index and ingest data
        String indexName = "test-index-11";
        Settings basicSettings = Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0).build();
        createIndex(indexName, basicSettings);
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field1", "the fox jumps in the well"));
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field2", "another fox did the same."));

        ensureGreen();
        refresh();

        // Make the search calls; adding the searchType and PreFilterShardSize to make the query path predictable across all the runs.
        client.prepareSearch().setSearchType("query_then_fetch").setPreFilterShardSize(3).setQuery(queryStringQuery("fox")).get();
        client.prepareSearch().setSearchType("query_then_fetch").setPreFilterShardSize(3).setQuery(queryStringQuery("jumps")).get();

        ensureGreen();
        refresh();

        // Sleep for about 3s to wait for traces are published, delay is (the delay is 1s).
        Thread.sleep(3000);

        TelemetryValidators validators = new TelemetryValidators(
            Arrays.asList(
                new AllSpansAreEndedProperly(),
                new AllSpansHaveUniqueId(),
                new NumberOfTraceIDsEqualToRequests(Attributes.create().addAttribute("action", "indices:data/read/search[phase/query]")),
                new TotalRootSpansEqualToRequests()
            )
        );

        InMemorySingletonSpanExporter exporter = InMemorySingletonSpanExporter.INSTANCE;
        if (!exporter.getFinishedSpanItems().isEmpty()) {
            validators.validate(exporter.getFinishedSpanItems(), 6);
        }
    }

    private static void updateTelemetrySetting(Client client, boolean value) {
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), value))
            .get();
    }

}
